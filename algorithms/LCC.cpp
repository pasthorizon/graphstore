//
// Created by per on 26.03.21.
//

#include "LCC.h"
#include "Algorithms.h"
#include <data-structure/VersionedBlockedEdgeIterator.h>

vector<pair<vertex_id_t, double>> LCC::lcc(Driver &driver, TopologyInterface &ds) {
  auto lcc_values = lcc_merge_sort(ds);
  return Algorithms::translate<double>(ds, lcc_values);
}

vector<double> LCC::lcc_merge_sort(TopologyInterface &ds) {
  if (typeid(ds) != typeid(SnapshotTransaction &)) {
    throw ConfigurationError("Cannot run sort merge lcc on any unsorted data structure");
  }

  auto N = ds.max_physical_vertex();

  unique_ptr<atomic<uint32_t>[]> p(new atomic<uint32_t>[N]());
  auto triangles_per_vertex = p.get();

#pragma omp parallel
  {
    vector<dst_t> a_neighbours;
    auto triangles_a = 0u;
    auto triangles_b = 0u;

#pragma omp for schedule(dynamic, 256)
    for (vertex_id_t a = 0; a < N; a++) {
      a_neighbours.clear();

      SORTLEDTON_ITERATE_NAMED(ds, a, b, end_a, {
        if (a < b) {  // We search triangles for which a > b.
          goto end_a;
        } else {
          a_neighbours.push_back(b);
          auto merge_marker = 0;

          SORTLEDTON_ITERATE_NAMED(ds, b, c, end_b, {
            if (b < c) {  // We search for triangles for which b > c.
              goto end_b;
            } else {
              auto e = lower_bound(a_neighbours.begin() + merge_marker, a_neighbours.end(), c);
              if (e == a_neighbours.end()) {
                break;
              } else if (*e == c) {
                merge_marker = e - a_neighbours.begin();
                triangles_a += 2;
                triangles_b += 2;
                triangles_per_vertex[c] += 2;
              }
            }
          });
        }
        triangles_per_vertex[b] += triangles_b;
        triangles_b = 0;
      });

      triangles_per_vertex[a] += triangles_a;
      triangles_a = 0;
    }
  }

  vector<double> lcc_values(N);

#pragma omp parallel for
  for (vertex_id_t v = 0; v < N; v++) {
    uint64_t degree = ds.neighbourhood_size_p(v);
    uint64_t max_num_edges = degree * (degree - 1);
    if (max_num_edges != 0) {
      lcc_values[v] = ((double) triangles_per_vertex[v]) / max_num_edges;
    } else {
      lcc_values[v] = 0.0;
    }
  }

  return lcc_values;
}
