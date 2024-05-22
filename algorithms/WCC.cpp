//
// Created by per on 26.03.21.
//

#include "WCC.h"
#include "Algorithms.h"
#include <data-structure/VersionedBlockedEdgeIterator.h>

/*
GAP Benchmark Suite
Kernel: Connected Components (CC)
Author: Scott Beamer

Will return comp array labelling each vertex with a connected component ID

This CC implementation makes use of the Shiloach-Vishkin [2] algorithm with
implementation optimizations from Bader et al. [1]. Michael Sutton contributed
a fix for directed graphs using the min-max swap from [3], and it also produces
more consistent performance for undirected graphs.

[1] David A Bader, Guojing Cong, and John Feo. "On the architectural
    requirements for efficient execution of graph algorithms." International
    Conference on Parallel Processing, Jul 2005.

[2] Yossi Shiloach and Uzi Vishkin. "An o(logn) parallel connectivity algorithm"
    Journal of Algorithms, 3(1):57–67, 1982.

[3] Kishore Kothapalli, Jyothish Soman, and P. J. Narayanan. "Fast GPU
    algorithms for graph connectivity." Workshop on Large Scale Parallel
    Processing, 2010.
*/

// The hooking condition (comp_u < comp_v) may not coincide with the edge's
// direction, so we use a min-max swap such that lower component IDs propagate
// independent of the edge's direction.

#pragma clang diagnostic push
#pragma ide diagnostic ignored "EndlessLoop"
vector<vertex_id_t> WCC::gapbs_wcc(TopologyInterface &ds) {
  const uint64_t V = ds.max_physical_vertex();
  vector<vertex_id_t> components(V);
#pragma omp parallel for
  for (uint64_t v = 0; v < V; v++) {
    components[v] = v;
  }

  bool change = true;
  int num_iters = 0;
  while (change) {
    change = false;

    // TODO define change as shared?
#pragma omp parallel for schedule(dynamic, 64)
      for (uint64_t v = 0; v < V; v++) {
        SORTLEDTON_ITERATE(ds, v, {
          if(e<V){
            uint64_t comp_v = components[v];
            uint64_t comp_n = components[e];
            if (comp_n != comp_v) {
              // Hooking condition so lower component ID wins independent of direction
              uint64_t high_comp = std::max(comp_n, comp_v);
              uint64_t low_comp = std::min(comp_n, comp_v);
              if (high_comp == components[high_comp]) {
                change = true;
                components[high_comp] = low_comp;
              }
            }
          }
        });
      }

#pragma omp parallel for schedule(dynamic, 64)
      for (uint64_t v = 0; v < V; v++) {
        while (components[v] != components[components[v]]) {
          components[v] = components[components[v]];
        }
      }
      num_iters++;
  }
  cout<<"\n\nnum iters: "<<num_iters<<endl;
  return components;
}
#pragma clang diagnostic pop

vector<pair<vertex_id_t, vertex_id_t>> WCC::wcc(Driver& driver, TopologyInterface &ds, bool run_on_raw_neighbourhoud) {
  if (run_on_raw_neighbourhoud) {
    throw NotImplemented();
  }
  vector<vertex_id_t> physical_results = gapbs_wcc( ds);

  auto start = chrono::steady_clock::now();
  vector<pair<vertex_id_t , vertex_id_t>> logical_result(physical_results.size());
  auto V = physical_results.size();

#pragma omp parallel for
  for (uint v = 0; v <  V; v++) {
    if (ds.has_vertex_p(v)) {
      logical_result[v] = make_pair(ds.logical_id(v), ds.logical_id(physical_results[v]));
    } else {
      logical_result[v] = make_pair(v, numeric_limits<vertex_id_t>::max());
    }
  }
  auto end = chrono::steady_clock::now();
  auto milliseconds = chrono::duration_cast<chrono::milliseconds>(end - start).count();

  cout << "Translating took: " << milliseconds << " milliseconds" << endl;
  return logical_result;
}