//
// Created by per on 26.03.21.
//

#include "SSSP.h"
#include "third-party/gapbs.h"
#include "data-structure/VersionedBlockedPropertyEdgeIterator.h"
#include "Algorithms.h"


vector<pair<vertex_id_t, weight_t>>
SSSP::sssp(TopologyInterface &ds, bool use_raw_neighbourhoud, uint64_t source_vertex_id, double delta) {
  if (use_raw_neighbourhoud) {
    throw NotImplemented();
  }
  auto distances = gabbs_sssp(ds, ds.physical_id(source_vertex_id), delta);
  return Algorithms::translate<weight_t>(ds, distances);
}


// Implementation based on the reference SSSP for the GAP Benchmark Suite
// https://github.com/sbeamer/gapbs
// The reference implementation has been written by Scott Beamer
//
// Copyright (c) 2015, The Regents of the University of California (Regents)
// All Rights Reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. Neither the name of the Regents nor the
//    names of its contributors may be used to endorse or promote products
//    derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL REGENTS BE LIABLE FOR ANY
// DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

vector<weight_t> SSSP::gabbs_sssp(TopologyInterface &ds, uint64_t physical_source, double delta) {
  const size_t kMaxBin = numeric_limits<size_t>::max() / 2;

  const uint64_t num_vertices = ds.max_physical_vertex();
  const uint64_t num_edges = ds.edge_count();

  // Init
  vector<weight_t> dist(num_vertices, numeric_limits<weight_t>::infinity());
  dist[physical_source] = 0;
  gapbs::pvector<vertex_id_t> frontier(num_edges);
  // two element arrays for double buffering curr=iter&1, next=(iter+1)&1
  size_t shared_indexes[2] = {0, kMaxBin};
  size_t frontier_tails[2] = {1, 0};

  frontier[0] = physical_source;

  SnapshotTransaction& tx = dynamic_cast<SnapshotTransaction&>(ds);
#pragma omp parallel
  {
    vector<vector<vertex_id_t> > local_bins(0);
    size_t iter = 0;

    while (shared_indexes[iter & 1] != kMaxBin) {
      size_t &curr_bin_index = shared_indexes[iter & 1];
      size_t &next_bin_index = shared_indexes[(iter + 1) & 1];
      size_t &curr_frontier_tail = frontier_tails[iter & 1];
      size_t &next_frontier_tail = frontier_tails[(iter + 1) & 1];

#pragma omp for nowait schedule(dynamic, 64)
      for (size_t i = 0; i < curr_frontier_tail; i++) {
        vertex_id_t u = frontier[i];
        if (dist[u] >= delta * static_cast<weight_t>(curr_bin_index)) {

          SORTLEDTON_ITERATE_WITH_PROPERTIES_NAMED(tx, u, v, w, end_iteration, {
            weight_t old_dist = dist[v];
            weight_t new_dist = dist[u] + w;

            if (new_dist < old_dist) {
              bool changed_dist = true;
              while (!gapbs::compare_and_swap(dist[v], old_dist, new_dist)) {
                old_dist = dist[v];
                if (old_dist <= new_dist) {
                  changed_dist = false;
                  break;
                }
              }
              if (changed_dist) {
                size_t dest_bin = new_dist / delta;
                if (dest_bin >= local_bins.size()) {
                  local_bins.resize(dest_bin + 1);
                }
                local_bins[dest_bin].push_back(v);
              }
            }

          });
        }
      }

      for (size_t i = curr_bin_index; i < local_bins.size(); i++) {
        if (!local_bins[i].empty()) {
#pragma omp critical
          next_bin_index = min(next_bin_index, i);
          break;
        }
      }

#pragma omp barrier
#pragma omp single nowait
      {
        curr_bin_index = kMaxBin;
        curr_frontier_tail = 0;
      }

      if (next_bin_index < local_bins.size()) {
        size_t copy_start = gapbs::fetch_and_add(next_frontier_tail, local_bins[next_bin_index].size());
        copy(local_bins[next_bin_index].begin(), local_bins[next_bin_index].end(), frontier.data() + copy_start);
        local_bins[next_bin_index].resize(0);
      }

      iter++;

#pragma omp barrier
    }

#if defined(DEBUG)
#pragma omp single
    cout << "took " << iter << " iterations";
#endif
  }
  return dist;
}
