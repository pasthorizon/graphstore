//
// Created by per on 09.10.20.
//

#include "Algorithms.h"
#include "PageRank.h"

#include <algorithm>
#include <queue>

#include "data-structure/data_types.h"
#include "GAPBSAlgorithms.h"
#include "internal-driver/Driver.h"
#include "WCC.h"
#include "CDLP.h"
#include "SSSP.h"
#include "LCC.h"

uint Algorithms::traversed_vertices(TopologyInterface &ds, vector<pair<vertex_id_t, uint>> &distances) {
  auto count = 0;
  for (auto d : distances) {
    if (d.second != numeric_limits<uint>::max()) {
      count++;
    }
  }
  return count;
}

vector<pair<vertex_id_t, uint>> Algorithms::bfs(Driver &driver, TopologyInterface &ds, vertex_id_t start_vertex, bool raw_neighbourhood,
                             bool aquire_locks, bool gapbs) {
  auto start = chrono::steady_clock::now();
  start_vertex = ds.physical_id(start_vertex);  // Logical to physical translation
  vector<uint> physical_result;
  vector<pair<vertex_id_t, uint>> logical_result;
  if (gapbs) {
    auto distances = GAPBSAlgorithms::bfs(ds, start_vertex, raw_neighbourhood);
    int N = distances.size();
    physical_result.resize(N);
#pragma omp parallel for
    for (int i = 0; i < N; i++) {
      if (distances[i] < 0) {
        physical_result[i] = numeric_limits<uint>::max();
      } else {
        physical_result[i] = distances[i];
      }
    }
  } else {
    throw NotImplemented();
  }

  auto end = chrono::steady_clock::now();
  size_t milliseconds = chrono::duration_cast<chrono::milliseconds>(end - start).count();

  cout << "BFS took: " << milliseconds << " milliseconds" << endl;

  return Algorithms::translate(ds, physical_result);
}

vector<pair<vertex_id_t, double>> Algorithms::page_rank(Driver &driver, TopologyInterface &ds, bool run_on_raw_neighbourhood, bool use_gapbs) {
  return PageRank::page_rank(driver, ds, driver.config.page_rank_max_iterations(), driver.config.page_rank_damping_factor(), run_on_raw_neighbourhood, use_gapbs);
}

vector<pair<vertex_id_t, weight_t>>
Algorithms::sssp(Driver &driver, TopologyInterface &ds, bool use_raw_neighbourhood) {
  return SSSP::sssp(ds, use_raw_neighbourhood, driver.sssp_start_vertex(ds), driver.config.sssp_delta());
}

vector<pair<vertex_id_t, vertex_id_t>>
Algorithms::wcc(Driver &driver, TopologyInterface &ds, bool use_raw_neighbourhood) {
  return WCC::wcc(driver, ds, use_raw_neighbourhood);
}

vector<pair<vertex_id_t, double>> Algorithms::lcc(Driver &driver, TopologyInterface &ds, bool use_raw_neighbourhood) {
  return LCC::lcc(driver, ds);
}

vector<pair<vertex_id_t, vertex_id_t>>
Algorithms::cdlp(Driver &driver, TopologyInterface &ds, bool use_raw_neighbourhood) {
  return CDLP::cdlp(driver, ds, driver.config.cdlp_max_iterations(), use_raw_neighbourhood);
}
