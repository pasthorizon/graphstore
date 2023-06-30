//
// Created by per on 09.10.20.
//

#ifndef LIVE_GRAPH_TWO_ALGORITHMS_H
#define LIVE_GRAPH_TWO_ALGORITHMS_H

#include <memory>
#include <vector>
#include <chrono>
#include <iostream>

#include "data-structure/TopologyInterface.h"

class Driver;

using namespace std;
class Algorithms {
public:
    static vector<pair<vertex_id_t, uint>> bfs(Driver &driver, TopologyInterface &ds, vertex_id_t start_vertex, bool raw_neighbourhood,
                            bool aquire_locks, bool gapbs);
    static vector<pair<vertex_id_t, uint>> bfs(Driver& driver, TopologyInterface& ds, vertex_id_t start_vertex) { return bfs(driver, ds,
                                                                                                          start_vertex,
                                                                                                          false, false,
                                                                                                          false); };

    static vector<pair<vertex_id_t , weight_t>> sssp(Driver& driver, TopologyInterface& ds, bool use_raw_neighbourhood);
    static vector<pair<vertex_id_t , vertex_id_t>> wcc(Driver& driver, TopologyInterface& ds, bool use_raw_neighbourhood);
    static vector<pair<vertex_id_t , double>> lcc(Driver& driver, TopologyInterface& ds, bool use_raw_neighbourhood);
    static vector<pair<vertex_id_t , vertex_id_t>> cdlp(Driver& driver, TopologyInterface& ds, bool use_raw_neighbourhood);

    static vector<pair<vertex_id_t, double>> page_rank(Driver& driver, TopologyInterface& ds, bool use_raw_neighbourhood, bool use_gapbs);

    static uint traversed_vertices(TopologyInterface& ds, vector<pair<vertex_id_t, uint>>& vector);

    template <typename T>
    static vector<pair<vertex_id_t , T>> translate(TopologyInterface& ds, vector<T>& values) {
      auto start = chrono::steady_clock::now();
      vector<pair<vertex_id_t , T>> logical_result(values.size());
      auto V = values.size();

#pragma omp parallel for
      for (uint v = 0; v <  V; v++) {
        if (ds.has_vertex_p(v)) {
          logical_result[v] = make_pair(ds.logical_id(v), values[v]);
        } else {
          logical_result[v] = make_pair(v, numeric_limits<T>::max());
        }
      }
      auto end = chrono::steady_clock::now();
      auto milliseconds = chrono::duration_cast<chrono::milliseconds>(end - start).count();

      cout << "Translating took: " << milliseconds << " milliseconds" << endl;
      return logical_result;
    }
};


#endif //LIVE_GRAPH_TWO_ALGORITHMS_H
