// K-nearest-neighbor (kNN) spatial query.
//
// CentroidKNNIndex: builds a static 2D k-d tree from the centroids of a set
// of WKB-encoded candidate geometries.  Centroid is the bounding-box centre,
// computed with wkb_bbox() — zero GEOS allocation at build or query time.
//
// When the candidates column is COL_IS_CONST (e.g. from a scalar subquery),
// the index is built once per batch and queried once per row.
//
// Query algorithm: branch-and-bound on the 2D k-d tree.
//   1. Maintain a max-heap of the k best (dist², idx) pairs found so far.
//   2. At each node, update the heap with the node's centroid distance.
//   3. Recursively search the near subtree first, then the far subtree only
//      if its minimum possible distance² is less than the current k-th best.
// Expected node visits: O(k · N^(1/2)) for uniform 2D data; far better than
// the O(N) worst case of the old expanding-envelope STRtree approach on dense
// datasets where the radius must expand many times before finding k results.

#pragma once

#include <algorithm>
#include <cmath>
#include <limits>
#include <span>
#include <utility>
#include <vector>

#include "../geom/wkb.hpp"
#include "../geom/wkb_envelope.hpp"

namespace ch {

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
    std::partial_sort(dists.begin(), dists.begin() + take, dists.end(),
        [](const auto& a, const auto& b) { return a.second < b.second; });
    dists.resize(take);
    return dists;
}

// Static 2D k-d tree over building centroids.
// Build: O(N log²N) using std::nth_element at each level.
// Query: O(log N) to O(√N) expected via branch-and-bound with a max-heap.
class CentroidKNNIndex
{
    struct Point { double x, y; uint64_t idx; };

    std::vector<Point> tree_; // partitioned in-place as an implicit k-d tree

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

    void search(size_t lo, size_t hi, int depth,
                double qx, double qy, uint32_t k,
                std::vector<std::pair<double,uint64_t>>& heap) const
    {
        if (hi <= lo) return;

        size_t mid = lo + (hi - lo) / 2;
        const Point& p = tree_[mid];

        double dx = qx - p.x, dy = qy - p.y;
        double d2 = dx * dx + dy * dy;

        if (heap.size() < k)
        {
            heap.emplace_back(d2, p.idx);
            std::push_heap(heap.begin(), heap.end(), heap_cmp);
        }
        else if (d2 < heap.front().first)
        {
            std::pop_heap(heap.begin(), heap.end(), heap_cmp);
            heap.back() = {d2, p.idx};
            std::push_heap(heap.begin(), heap.end(), heap_cmp);
        }

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

        search(near_lo, near_hi, depth + 1, qx, qy, k, heap);

        // Prune the far subtree if its closest possible point is already
        // farther than the current k-th best distance².
        double worst = (heap.size() < k)
            ? std::numeric_limits<double>::infinity()
            : heap.front().first;
        if (split_delta * split_delta < worst)
            search(far_lo, far_hi, depth + 1, qx, qy, k, heap);
    }

public:
    explicit CentroidKNNIndex(const std::vector<std::span<const uint8_t>>& wkbs)
    {
        tree_.reserve(wkbs.size());
        for (uint64_t i = 0; i < static_cast<uint64_t>(wkbs.size()); ++i)
        {
            BBox bb = wkb_bbox(wkbs[i]);
            if (bb.is_empty()) continue;
            tree_.push_back({(bb.xmin + bb.xmax) * 0.5,
                             (bb.ymin + bb.ymax) * 0.5,
                             i});
        }
        build(0, tree_.size(), 0);
    }

    bool empty() const { return tree_.empty(); }

    // Returns k (idx, distance) pairs sorted by centroid distance ascending.
    std::vector<std::pair<uint64_t, double>>
    query(double qx, double qy, uint32_t k) const
    {
        if (empty() || k == 0) return {};

        std::vector<std::pair<double,uint64_t>> heap;
        heap.reserve(k);
        search(0, tree_.size(), 0, qx, qy, k, heap);

        std::vector<std::pair<uint64_t, double>> result;
        result.reserve(heap.size());
        for (const auto& [d2, idx] : heap)
            result.emplace_back(idx, std::sqrt(d2));
        std::sort(result.begin(), result.end(),
                  [](const auto& a, const auto& b) { return a.second < b.second; });
        return result;
    }
};

} // namespace ch
