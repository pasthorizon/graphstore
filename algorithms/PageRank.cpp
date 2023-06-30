//
// Created by per on 03.12.20.
//

#include "PageRank.h"
#include <third-party/gapbs.h>
#include <data-structure/VersionedBlockedEdgeIterator.h>
#include "Algorithms.h"

vector<pair<vertex_id_t, double>>
PageRank::page_rank(Driver &driver, TopologyInterface &ds, int iterations, double damping_factor,
                    bool use_raw_neighbourhood, bool use_gapbs) {
  if (!use_gapbs || use_raw_neighbourhood) {
    throw NotImplemented();
  }
  vector<double> scores = page_rank_bs(ds, iterations, damping_factor);;
  return Algorithms::translate(ds, scores);
}

// Implementation based on the reference PageRank for the GAP Benchmark Suite
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

/*
GAP Benchmark Suite
Kernel: PageRank (PR)
Author: Scott Beamer

Will return pagerank scores for all vertices once total change < epsilon

This PR implementation uses the traditional iterative approach. This is done
to ease comparisons to other implementations (often use same algorithm), but
it is not necesarily the fastest way to implement it. It does perform the
updates in the pull direction to remove the need for atomics.
*/

// The error computation has been removed and the concept of dangling sum has been added from the original GAPBS implementation.
vector<double> PageRank::page_rank_bs(TopologyInterface &ds, int num_iterations, double damping_factor) {
  const uint64_t num_vertices = ds.vertex_count();
  const uint64_t max_physical_vertices = ds.max_physical_vertex();

  const double init_score = 1.0 / num_vertices;
  const double base_score = (1.0 - damping_factor) / num_vertices;

  vector<double> scores(max_physical_vertices);

#pragma omp parallel for
  for (uint64_t v = 0; v < max_physical_vertices; v++) {
    scores[v] = init_score;
  }

  gapbs::pvector<double> outgoing_contrib(max_physical_vertices, 0.0);

  // pagerank iterations
  for (int iteration = 0; iteration < num_iterations; iteration++) {
    double dangling_sum = 0.0;

    // for each node, precompute its contribution to all of its outgoing neighbours and, if it's a sink,
    // add its rank to the `dangling sum' (to be added to all nodes).
#pragma omp parallel for reduction(+:dangling_sum)
    for (uint64_t v = 0; v < max_physical_vertices; v++) {
      uint64_t out_degree = ds.neighbourhood_size_p(v);
      if (out_degree == 0) { // this is a sink
        dangling_sum += scores[v];
      } else {
        outgoing_contrib[v] = scores[v] / out_degree;
      }
    }

    dangling_sum /= num_vertices;

    // compute the new score for each node in the graph
#pragma omp parallel for schedule(dynamic, 64)
    for (uint64_t v = 0; v < max_physical_vertices; v++) {
      double incoming_total = 0;
      SORTLEDTON_ITERATE(ds, v, {
        incoming_total += outgoing_contrib[e];
      });
      scores[v] = base_score + damping_factor * (incoming_total + dangling_sum);
    }
  }

  return scores;
}
