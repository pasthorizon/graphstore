//
// Created by per on 31.08.20.
//

#ifndef LIVE_GRAPH_TWO_DRIVER_H
#define LIVE_GRAPH_TWO_DRIVER_H

#include <memory>

#include "data-structure/TopologyInterface.h"
#include "data-src/EdgeList.h"
#include <data-structure/TransactionManager.h>
#include <data-structure/VersionedTopologyInterface.h>
#include "Reporter.h"
#include "Configuration.h"

namespace specialize {
    template <typename T>
    inline bool check_equal(T a, T b, double tolerance) {
      return a == b;
    }

    template<>
    inline bool check_equal(double a, double b, double tolerance) {
      if (isinf(a) && isinf(b)) {
        return true;
      }
      if (isnan(a) && isnan(b)) {
        return true;
      }
      return fabs(a - b) < tolerance;
    }
}


class Driver {
public:
    Driver(Config config) : config(config), reporter(config) { };

    void run();

    Config config;

    vertex_id_t sssp_start_vertex(TopologyInterface& ds);

private:
    Reporter reporter;
    SortedCSRDataSource read_base_dataset();
    EdgeList<weighted_edge_t> read_insert_dataset();

    void run_data_structure(SortedCSRDataSource& base, EdgeList<weighted_edge_t>& inserts,
                            DataStructures ds,
                            const vector<string>& ds_parameters,
                            vector<vector<vertex_id_t>>& neighbour_2_sources);

    void load_base_dataset(TopologyInterface& ds, SortedCSRDataSource& base);

    void run_insert_experiment_one_by_one(TransactionManager &tm, VersionedTopologyInterface *ds, EdgeList<weighted_edge_t> &el,
                                          size_t base_edge_count);
    void check_insert(TopologyInterface& ds, EdgeList<weighted_edge_t>& el, size_t base_edge_count);

    void run_delete_experiment(TransactionManager &tm, VersionedTopologyInterface *ds,
                               EdgeList<weighted_edge_t> &existing_edges, size_t expected_edge_count_after_reinsertion);
    void check_deletions(TopologyInterface& ds, EdgeList<weighted_edge_t>& el);

    void run_bfs_experiment(TopologyInterface &ds, bool run_on_raw_neighbourhood, bool aquire_locks, bool after_inserts,
                            bool gabbs);

    void run_page_rank_experiment(TopologyInterface& ds, bool run_on_raw_neighbourhood, bool gapbs);

    void show_storage_sizes(string ds_name, TopologyInterface& ds);

    void run_gc_experiment(TransactionManager& tm, VersionedTopologyInterface& ds, bool inserts_run, EdgeList<weighted_edge_t> &inserts);
    void check_gc_experiment(VersionedTopologyInterface& ds);

private:
    void run_analytics(Experiments e, TopologyInterface& ds, bool run_on_raw_neighbourhoud);

    template <typename T>
    void check_analytics(Experiments ex, vector<pair<vertex_id_t, T>> values) {
      string experiment_name = Config::EXPERIMENT_MAPPING.find(ex)->second;

      cout << "Validating " << experiment_name << " experiment" << endl;

      sort(values.begin(), values.end());

      string inserts = "base";

      const string gold_standard_file = config.gold_standard(ex);
      if (!file_exists(gold_standard_file)) {
        cout << "Writing new gold standard for: " << gold_standard_file << endl;
        ofstream f(gold_standard_file, ofstream::binary | ofstream::out);

        if (!f.good()) {
          assert(false);
        }

        size_t size = values.size();
        f.write((char *) &size, sizeof(size));

        for (auto s : values) {
          f.write((char *) &s.first, sizeof(s.first));
          f.write((char *) &s.second, sizeof(s.second));
        }
        f.close();
      } else {
        ifstream f(gold_standard_file, ifstream::in | ifstream::binary);

        size_t size;
        f.read((char *) &size, sizeof(size));
        assert(size == values.size());

        vertex_id_t v = 0;
        T e = 0.0;
        auto errors = 0;

        auto tolerance = config.page_rank_error();
        for (auto i = 0u; i < values.size(); i++) {
          auto d = values[i];
          f.read((char*) &v, sizeof(v));
          f.read((char *) &e, sizeof(T));

//          cout << v << " " << e << endl;

          auto correct = check_equal(d.second, e, tolerance);

          assert(v == d.first);
//          if (i < 100) {
//            cout << d.second << endl;
//          }
          if (!correct && errors < 100) {
            errors += 1;
            cout << i << "Actual: " << d.second << "Expected: " << e << "Difference: " << fabs(d.second - e) << endl;
          }
          // TODO should I implement WCC checking?
//          assert(correct);
        }
        f.close();
      }
    };

    template <typename T>
    inline bool check_equal(T a, T b, double tolerance) {
      return specialize::check_equal(a, b, tolerance);
    }

    EdgeList<weighted_edge_t> generate_deletions(EdgeList<weighted_edge_t>& existing_edges, double deletion_percentage);

};

#endif //LIVE_GRAPH_TWO_DRIVER_H
