// K-nearest-neighbor (kNN) spatial query.
//
// KNNIndex: builds a GEOS TemplateSTRtree from a set of WKB-encoded candidate
// geometries.  When the candidates column is COL_IS_CONST (e.g. from a scalar
// subquery), the index is built once per batch and queried once per row.
//
// Query algorithm: expanding-envelope search.
//   1. Search the tree with an envelope expanded by radius r.
//   2. Compute exact distance for each hit.
//   3. If we have ≥ k hits and the k-th distance ≤ r, we have the true kNN.
//   4. Otherwise expand r to the k-th exact distance and repeat.
// This guarantees correctness because a geometry with actual distance d from
// the query must have an envelope that intersects a search radius of d.

#pragma once

#include <algorithm>
#include <span>
#include <utility>
#include <vector>

#include <geos/geom/Envelope.h>
#include <geos/index/strtree/EnvelopeUtil.h>
#include <geos/index/strtree/TemplateSTRtree.h>

#include "../geom/wkb.hpp"

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

// STRtree-backed kNN index.  Built once from a set of candidate WKBs.
class KNNIndex
{
    using Tree = geos::index::strtree::TemplateSTRtree<uint64_t>;

    mutable Tree                                   tree_;
    std::vector<std::unique_ptr<geos::geom::Geometry>> geoms_;

public:
    explicit KNNIndex(const std::vector<std::span<const uint8_t>>& wkbs)
    {
        geoms_.reserve(wkbs.size());
        for (uint64_t i = 0; i < static_cast<uint64_t>(wkbs.size()); ++i)
        {
            auto g = read_wkb(wkbs[i]);
            if (!g) continue;
            tree_.insert(*g->getEnvelopeInternal(), i);
            geoms_.push_back(std::move(g));
        }
        tree_.build();
    }

    bool empty() const { return geoms_.empty(); }

    std::vector<std::pair<uint64_t, double>>
    query(const geos::geom::Geometry* q, uint32_t k) const
    {
        if (geoms_.empty() || k == 0) return {};

        const geos::geom::Envelope& q_env = *q->getEnvelopeInternal();
        double r = 0.0;

        for (int attempt = 0; attempt < 64; ++attempt)
        {
            geos::geom::Envelope search = q_env;
            if (r > 0.0) search.expandBy(r);

            std::vector<std::pair<uint64_t, double>> found;
            tree_.query(search, [&](uint64_t idx) {
                double d = q->distance(geoms_[idx].get());
                found.push_back({idx, d});
            });

            if (found.size() >= k)
            {
                uint32_t take = std::min(k, static_cast<uint32_t>(found.size()));
                std::partial_sort(found.begin(), found.begin() + take, found.end(),
                    [](const auto& a, const auto& b) { return a.second < b.second; });
                double dk = found[k - 1].second;
                if (dk <= r)
                {
                    // Search radius covers the k-th nearest → result is correct.
                    found.resize(take);
                    return found;
                }
                // Expand to exactly cover the k-th nearest distance.
                r = dk;
            }
            else
            {
                // Too few candidates; double the radius.
                r = (r == 0.0) ? 1e-10 : r * 2.0;
            }
        }

        // Fallback: return whatever we accumulated in the last iteration.
        // (Shouldn't happen for non-pathological data.)
        geos::geom::Envelope search = q_env;
        search.expandBy(r);
        std::vector<std::pair<uint64_t, double>> found;
        tree_.query(search, [&](uint64_t idx) {
            found.push_back({idx, q->distance(geoms_[idx].get())});
        });
        uint32_t take = std::min(k, static_cast<uint32_t>(found.size()));
        std::partial_sort(found.begin(), found.begin() + take, found.end(),
            [](const auto& a, const auto& b) { return a.second < b.second; });
        found.resize(take);
        return found;
    }
};

} // namespace ch
