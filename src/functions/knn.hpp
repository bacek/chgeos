// K-nearest-neighbor (kNN) spatial query.
//
// CentroidKNNIndex: builds a static 2D k-d tree from the centroids of a set
// of WKB-encoded candidate geometries.  Centroid is the bounding-box centre,
// computed with wkb_bbox() — zero GEOS allocation at build time.
//
// When the candidates column is COL_IS_CONST (e.g. from a scalar subquery),
// the index is built once per batch and queried once per row.
//
// Centroid distance is only an approximation of geometry distance, so the tree
// selects candidates and an exact refinement step decides the answer:
//
//   1. Branch-and-bound over the k-d tree, maintaining a max-heap of the k best
//      centroid distances², exactly as a plain centroid kNN would.
//   2. Because a geometry lies within r_i (its bbox half-diagonal) of its own
//      centroid, true distance is bracketed by d_c ± r_i.  So every candidate
//      whose centroid falls within (d_k + 2·r_max) may still be a true nearest
//      neighbour: those are collected, and subtrees are pruned against that
//      inflated radius rather than d_k.
//   3. The collected set — k plus a handful, since footprints are small next to
//      the spacing between them — gets an exact distance and is then sorted.
//
// The result is exact, and the added cost is a short point-to-segment walk per
// collected candidate (see geom/wkb_distance.hpp), with no GEOS parse when the
// query is a point.  Expected node visits stay O(k · N^(1/2)) for uniform 2D
// data, far better than the O(N) worst case of the old expanding-envelope
// STRtree approach on dense datasets where the radius must expand many times
// before finding k results.

#pragma once

#include <algorithm>
#include <cmath>
#include <limits>
#include <span>
#include <utility>
#include <vector>

#include "../geom/wkb.hpp"
#include "../geom/wkb_distance.hpp"
#include "../geom/wkb_envelope.hpp"

namespace ch {

// Order kNN output by distance, breaking ties on candidate index so that equal
// distances produce a stable, reproducible answer.
inline bool knn_result_less(const std::pair<uint64_t, double>& a,
                            const std::pair<uint64_t, double>& b)
{
    return a.second != b.second ? a.second < b.second : a.first < b.first;
}

// Brute-force kNN: parse each candidate WKB and sort by distance.
// Used when the candidates array varies per row (can't amortize index build).
inline std::vector<std::pair<uint64_t, double>>
st_knn_brute(const geos::geom::Geometry* q,
             const std::vector<std::span<const uint8_t>>& cands,
             uint32_t k)
{
    std::vector<std::pair<uint64_t, double>> dists;
    dists.reserve(cands.size());
    for (uint64_t i = 0; i < cands.size(); ++i)
    {
        auto g = read_wkb(cands[i]);
        if (g) dists.push_back({i, q->distance(g.get())});
    }
    uint32_t take = std::min(k, static_cast<uint32_t>(dists.size()));
    std::partial_sort(dists.begin(), dists.begin() + take, dists.end(), knn_result_less);
    dists.resize(take);
    return dists;
}

// Static 2D k-d tree over candidate centroids, with exact distance refinement.
// Build: O(N log²N) using std::nth_element at each level.
// Query: O(log N) to O(√N) expected via branch-and-bound with a max-heap.
class CentroidKNNIndex
{
    struct Point {
        double x, y;
        double r;     // bbox half-diagonal: |true distance − centroid distance| ≤ r
        uint64_t idx;
    };

    std::vector<Point> tree_; // partitioned in-place as an implicit k-d tree
    std::vector<std::span<const uint8_t>> wkbs_; // candidate bytes, for GEOS queries
    double r_max_ = 0.0;

    // Candidate coordinates, decoded once at build time: refining a row is then
    // arithmetic over ring_span_[idx] instead of a WKB walk per candidate.
    std::vector<double> coords_;
    std::vector<RingRef> rings_;
    std::vector<std::pair<uint32_t, uint32_t>> ring_span_; // per candidate: [first, last)

    // Traversal state: the k-best centroid heap plus the candidates that the
    // bracket says could still win.
    struct Search {
        double qx, qy;
        uint32_t k;
        std::vector<std::pair<double, uint64_t>> heap; // max-heap of centroid d²
        std::vector<uint64_t> collected;
        double collect_r2 = std::numeric_limits<double>::infinity();
    };

    // Comparators for alternating split dimensions.
    static bool cmp_x(const Point& a, const Point& b) { return a.x < b.x; }
    static bool cmp_y(const Point& a, const Point& b) { return a.y < b.y; }

    void build(size_t lo, size_t hi, int depth)
    {
        if (hi <= lo + 1) return;
        size_t mid = lo + (hi - lo) / 2;
        std::nth_element(tree_.begin() + lo, tree_.begin() + mid,
                         tree_.begin() + hi,
                         depth % 2 == 0 ? cmp_x : cmp_y);
        build(lo, mid, depth + 1);
        build(mid + 1, hi, depth + 1);
    }

    // Max-heap comparator: highest d² at front so we can check/remove the worst.
    static bool heap_cmp(const std::pair<double,uint64_t>& a,
                         const std::pair<double,uint64_t>& b)
    { return a.first < b.first; }

    // Once k centroids are in hand, the k-th true distance is at most
    // √worst + r_max, and a candidate can only beat that if its own centroid is
    // within one more r_max.  Recomputed only when the heap top moves, so the
    // sqrt is paid a handful of times per query rather than per node visited.
    void refresh_collect_radius(Search& s) const
    {
        if (s.heap.size() < s.k) return;
        double radius = std::sqrt(s.heap.front().first) + 2.0 * r_max_;
        s.collect_r2 = radius * radius;
    }

    void __attribute__((noinline)) search(size_t lo, size_t hi, int depth, Search& s) const
    {
        if (hi <= lo) return;

        size_t mid = lo + (hi - lo) / 2;
        const Point& p = tree_[mid];

        double dx = s.qx - p.x, dy = s.qy - p.y;
        double d2 = dx * dx + dy * dy;

        if (s.heap.size() < s.k)
        {
            s.heap.emplace_back(d2, p.idx);
            std::push_heap(s.heap.begin(), s.heap.end(), heap_cmp);
            refresh_collect_radius(s);
        }
        else if (d2 < s.heap.front().first)
        {
            std::pop_heap(s.heap.begin(), s.heap.end(), heap_cmp);
            s.heap.back() = {d2, p.idx};
            std::push_heap(s.heap.begin(), s.heap.end(), heap_cmp);
            refresh_collect_radius(s);
        }

        // The collect radius only ever shrinks, so a node rejected here can
        // never come back into contention later.
        if (d2 <= s.collect_r2)
            s.collected.push_back(p.idx);

        // Choose near vs far subtree based on which side of the split qx/qy is on.
        double split_delta = (depth % 2 == 0) ? dx : dy;
        size_t near_lo, near_hi, far_lo, far_hi;
        if (split_delta <= 0.0)
        {
            near_lo = lo;      near_hi = mid;
            far_lo  = mid + 1; far_hi  = hi;
        }
        else
        {
            near_lo = mid + 1; near_hi = hi;
            far_lo  = lo;      far_hi  = mid;
        }

        search(near_lo, near_hi, depth + 1, s);

        // Prune the far subtree only if even its closest possible geometry
        // (centroid distance minus r_max) is out of contention.
        if (split_delta * split_delta <= s.collect_r2)
            search(far_lo, far_hi, depth + 1, s);
    }

public:
    explicit CentroidKNNIndex(const std::vector<std::span<const uint8_t>>& wkbs)
        : wkbs_(wkbs)
    {
        tree_.reserve(wkbs.size());
        ring_span_.resize(wkbs.size(), {0, 0});

        // Most WKB bytes are coordinates, 16 per xy pair, so total size / 16 is
        // a close upper-ish estimate of the pair count — one allocation instead
        // of a growth sequence over millions of doubles.
        size_t total_bytes = 0;
        for (const auto & w : wkbs) total_bytes += w.size();
        coords_.reserve(2 * (total_bytes / 16));

        for (uint64_t i = 0; i < static_cast<uint64_t>(wkbs.size()); ++i)
        {
            if (wkbs[i].empty()) continue;
            BBox bb = wkb_bbox(wkbs[i]);
            if (bb.is_empty()) continue;

            uint32_t first = static_cast<uint32_t>(rings_.size());
            flatten_wkb_rings(wkbs[i], coords_, rings_);
            ring_span_[i] = {first, static_cast<uint32_t>(rings_.size())};

            double hx = (bb.xmax - bb.xmin) * 0.5;
            double hy = (bb.ymax - bb.ymin) * 0.5;
            double r  = std::sqrt(hx * hx + hy * hy);
            r_max_ = std::max(r_max_, r);
            tree_.push_back({(bb.xmin + bb.xmax) * 0.5,
                             (bb.ymin + bb.ymax) * 0.5,
                             r, i});
        }
        build(0, tree_.size(), 0);
    }

    bool empty() const { return tree_.empty(); }

    // Returns the k (idx, distance) pairs nearest to the query geometry,
    // sorted by exact distance ascending.
    std::vector<std::pair<uint64_t, double>>
    query(std::span<const uint8_t> query_wkb, uint32_t k) const
    {
        if (empty() || k == 0) return {};
        BBox qb = wkb_bbox(query_wkb);
        if (qb.is_empty()) return {};

        Search s{.qx = (qb.xmin + qb.xmax) * 0.5,
                 .qy = (qb.ymin + qb.ymax) * 0.5,
                 .k  = k};
        s.heap.reserve(k);
        s.collected.reserve(k * 2);
        search(0, tree_.size(), 0, s);

        // A point query needs no GEOS at all: its bbox centre is the point, so
        // the refinement is a walk over the candidate's own coordinates.
        // Anything else falls back to GEOS, but only for the collected few.
        bool point_query = wkb_is_point(query_wkb);
        std::unique_ptr<geos::geom::Geometry> qgeom;
        if (!point_query)
        {
            qgeom = read_wkb(query_wkb);
            if (!qgeom) return {};
        }

        std::vector<std::pair<uint64_t, double>> result;
        result.reserve(s.collected.size());
        for (uint64_t idx : s.collected)
        {
            if (point_query)
            {
                auto [first, last] = ring_span_[idx];
                result.emplace_back(idx, flat_rings_point_distance(
                    coords_, rings_, first, last, s.qx, s.qy));
            }
            else
            {
                auto g = read_wkb(wkbs_[idx]);
                if (g) result.emplace_back(idx, qgeom->distance(g.get()));
            }
        }

        uint32_t take = std::min(k, static_cast<uint32_t>(result.size()));
        std::partial_sort(result.begin(), result.begin() + take, result.end(),
                          knn_result_less);
        result.resize(take);
        return result;
    }
};

// Index-based kNN over a candidate set: build a k-d tree from candidate bbox
// centroids, then query it with exact distance refinement.
inline std::vector<std::pair<uint64_t, double>>
st_knn_indexed(std::span<const uint8_t> query_wkb,
               const std::vector<std::span<const uint8_t>>& cands,
               uint32_t k)
{
    if (k == 0 || cands.empty()) return {};
    CentroidKNNIndex idx(cands);
    return idx.query(query_wkb, k);
}

} // namespace ch
