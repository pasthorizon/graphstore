//
// Created by per on 26.03.21.
//

#include "CDLP.h"
#include "Algorithms.h"

#include <data-structure/VersionedBlockedEdgeIterator.h>


vector<vertex_id_t> CDLP::teseo_cdlp(TopologyInterface &ds, uint64_t max_iterations) {
  const uint64_t num_vertices = ds.max_physical_vertex();
  vector<vertex_id_t> labels0(num_vertices);
  vector<vertex_id_t> labels1(num_vertices);

#pragma omp parallel for
  for (vertex_id_t v = 0; v < num_vertices; v++) {
    labels0[v] = ds.logical_id(v);
  }

  // algorithm pass
  bool change = true;
  uint64_t current_iteration = 0;
  while (current_iteration < max_iterations && change) {
    change = false; // reset the flag

#pragma omp parallel for schedule(dynamic, 64) shared(change)
    for (uint64_t v = 0; v < num_vertices; v++) {
      unordered_map<uint64_t, uint64_t> histogram;

      // compute the histogram from both the outgoing & incoming edges. The aim is to find the number of each label
      // shared among the neighbours of node_id

      SORTLEDTON_ITERATE(ds, v, {
        histogram[labels0[e]]++;
      });

      // get the max label
      uint64_t label_max = numeric_limits<int64_t>::max();
      uint64_t count_max = 0;
      for (const auto pair : histogram) {
        if (pair.second > count_max || (pair.second == count_max && pair.first < label_max)) {
          label_max = pair.first;
          count_max = pair.second;
        }
      }

      labels1[v] = label_max;
      change |= (labels0[v] != labels1[v]);
    }

    labels0.swap(labels1);
    current_iteration++;
  }

  cout << "Iterations taken " << current_iteration << endl;
  return labels0;

}

vector<pair<vertex_id_t, vertex_id_t>>
CDLP::cdlp(Driver &driver, TopologyInterface &ds, uint64_t max_iterations, bool run_on_raw_neighbourhoud) {
  if (run_on_raw_neighbourhoud) {
    throw NotImplemented();
  }

  cout << "Starting CDLP" << endl;
  vector<vertex_id_t> physical_results = teseo_cdlp(ds, max_iterations);

  return Algorithms::translate(ds, physical_results);
}
