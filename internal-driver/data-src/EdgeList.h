//
// Created by per on 31.08.20.
//

#ifndef LIVE_GRAPH_TWO_EDGELIST_H
#define LIVE_GRAPH_TWO_EDGELIST_H

#include <unordered_map>
#include <vector>
#include <string>
#include <fstream>
#include <omp.h>
#include <random>
#include <utils/NotImplemented.h>
#include "data-structure/data_types.h"
#include "DataSource.h"

using namespace std;

template <typename et>
class EdgeList : DataSource {
    static mt19937 weight_generator;

public:
    typename vector<et>::iterator begin() { return edges.begin(); };
    typename vector<et>::iterator end() { return edges.end(); };

    vector<et> edges;

    void read_from_binary_file(const string& path) {
      ifstream f (path, ifstream::in | ifstream::binary);

      size_t edge_count;
      f.read((char*) &edge_count, sizeof(edge_count));

      edges.clear();
      edges.resize(edge_count);

      f.read((char*) edges.data(), edge_count * sizeof(et));

      f.close();
    };

    unordered_multimap<vertex_id_t, dst_t> to_map() {
      unordered_multimap<vertex_id_t, dst_t> m;
      m.reserve(edges.size());

      for (auto e : edges) {
        m.emplace(e.src, e.dst);
      }
      return m;
    };

    EdgeList<weighted_edge_t> add_weights(bool generate_weights) {
      // TODO could be optimized for the unweighted case.
      // TODO we could parallelize weight gneration for the dst_t case.
        EdgeList<weighted_edge_t> with_weights;
        with_weights.edges.resize(edges.size());

        for (auto i = 0u; i < edges.size(); i++) {
          auto e = edges[i];
          weight_t w = generate_weight(e);
          with_weights.edges[i] = weighted_edge_t(e.src, e.dst, w);
        }
        return with_weights;
    }

    weight_t generate_weight(edge_t e) {
      if (typeid(weight_t) == typeid(double)) {
        return uniform_real_distribution<double>(0.01, 1.0)(weight_generator);
      } else if (typeid(weight_t) == typeid(dst_t)) {
        return e.dst;
      } else {
        throw NotImplemented();
      }

    }
};

template <typename T>
mt19937 EdgeList<T>::weight_generator = mt19937(42);

#endif //LIVE_GRAPH_TWO_EDGELIST_H
