//
// Created by per on 31.08.20.
//

#include "internal-driver/Driver.h"

#include <memory>
#include <iostream>
#include <chrono>
#include <random>
#include <iomanip>
#include <omp.h>
#include <cassert>
#include <thread>
#include <atomic>
#include <exception>

#include <data-structure/SnapshotTransaction.h>
#include <data-structure/TransactionManager.h>
#include <data-structure/VersioningBlockedSkipListAdjacencyList.h>
#include <data-structure/VersionedBlockedPropertyEdgeIterator.h>
#include "BFSSourceSelector.h"
#include "algorithms/Algorithms.h"


#define CHECKINSERT 1

void Driver::run() {
  if (config.omp_threads != 0) {
    cout << "Setting OMP thread number to " << config.omp_threads << endl;
    omp_set_num_threads(config.omp_threads);
  }
#pragma omp parallel
  {
    if (omp_get_thread_num() == 0) {
      cout << "Using " << omp_get_num_threads() << " OMP threads." << endl;
    }
  }


  cout << "Starting to run experiments." << endl;

  cout << "Reading base dataset " << config.base.path << endl;
  SortedCSRDataSource base = read_base_dataset();
  cout << "Vertices" << base.vertex_count() << endl;

  EdgeList<weighted_edge_t> inserts;
  if (config.experiment_set.find(INSERT_TRANSACTIONS) != config.experiment_set.end()) {
    cout << "Reading insert dataset " << config.insertions.path << endl;
    inserts = read_insert_dataset();
  }

  vector<vector<vertex_id_t>> neighbour_2_sources;

  reporter.set_dataset(config.base);

  for (const auto &ds : config.data_structures) {
    cout << "Running data structure: " << ds.first << endl;
    run_data_structure(base, inserts, ds.first, ds.second, neighbour_2_sources);
  }
}

void
Driver::run_data_structure(SortedCSRDataSource &base, EdgeList<weighted_edge_t> &inserts,
                           DataStructures ds, const vector<string> &ds_parameters,
                           vector<vector<vertex_id_t>> &neighbourhood_2_sources) {
  reporter.set_data_structure(ds, ds_parameters);

  TopologyInterface *data_structure;
  string ds_name;

  TransactionManager tm(config.insert_threads + 1);
  tm.register_thread(0);
  VersionedTopologyInterface *versioned_data_structure = nullptr;
  SnapshotTransaction transaction(&tm, false, nullptr);
  tm.transactionCompleted(transaction);

  switch (ds) {
    case VERSIONED: {
      size_t block_size = 128;
      if (!ds_parameters.empty()) {  // TODO better parameter sanitization
        block_size = stoi(ds_parameters[0]);
      }
      versioned_data_structure = new VersioningBlockedSkipListAdjacencyList(block_size,
                                                                            config.weighted ? sizeof(weight_t) : 0, tm);
      ds_name = "versioned";
      break;
    }
    default: {
      throw ConfigurationError("Unknown data structure: " + ds);
    }
  }

  if (versioned_data_structure != nullptr) {
    transaction = tm.getSnapshotTransaction(versioned_data_structure, false);
    data_structure = &transaction;
  }

  if (!config.undirected && versioned_data_structure == nullptr) {
    cout << "Loading base dataset." << endl;
    load_base_dataset(*data_structure, base);
    if (config.undirected) {
      cerr << "Warning: Loading possible directed base data set even though we work in an undirected setting" << endl;
    }
  }

  if (versioned_data_structure != nullptr) {
    tm.transactionCompleted(transaction);
  }

  bool run_on_raw_neighbourhood = false;
  if (find(ds_parameters.begin(), ds_parameters.end(), "raw") != ds_parameters.end()) {
    run_on_raw_neighbourhood = true;
  }

  bool inserts_run = false;

  auto expected_edge_count_after_inserts = config.undirected ? inserts.edges.size() * 2 : inserts.edges.size() +
                                                                                          base.edge_count();
  for (auto e : config.experiments) {
    if (versioned_data_structure != nullptr) {
      transaction = tm.getSnapshotTransaction(versioned_data_structure, false);
      data_structure = &transaction;
    }

    bool gapbs = false;
    switch (e.first) {
      case (GAPBS_BFS):
        gapbs = true;  // Fallthrough
      case (BFS): {
        bool aquire_locks = false;
        if (!e.second.empty()) {
          aquire_locks = stoi(e.second[0]);
        }
        run_bfs_experiment(*data_structure, run_on_raw_neighbourhood, aquire_locks, inserts_run, gapbs);
        if (versioned_data_structure != nullptr) {
          tm.transactionCompleted(transaction);
        }
        break;
      }
      case GAPBS_PR:
        gapbs = true; // Fallthrough
      case (PR): {
        run_page_rank_experiment(*data_structure, run_on_raw_neighbourhood, gapbs);
        if (versioned_data_structure != nullptr) {
          tm.transactionCompleted(transaction);
        }
        break;
      }
      case (SSSP): {
        if (typeid(weight_t) != typeid(double)) {
          throw ConfigurationError("Cannot run SSSP with weight type other than double.");
        }
        if (!config.weighted) {
          throw ConfigurationError("SSSP can only be run on weighted graphs.");
        }
        run_analytics(e.first, *data_structure, run_on_raw_neighbourhood);
        if (versioned_data_structure != nullptr) {
          tm.transactionCompleted(transaction);
        }
        break;
      }
      case (WCC): {
        run_analytics(e.first, *data_structure, run_on_raw_neighbourhood);
        if (versioned_data_structure != nullptr) {
          tm.transactionCompleted(transaction);
        }

        break;
      }
      case (LCC): {
        run_analytics(e.first, *data_structure, run_on_raw_neighbourhood);
        if (versioned_data_structure != nullptr) {
          tm.transactionCompleted(transaction);
        }
        break;
      }
      case (CDLP): {
        run_analytics(e.first, *data_structure, run_on_raw_neighbourhood);
        if (versioned_data_structure != nullptr) {
          tm.transactionCompleted(transaction);
        }
        break;
      }
      case (INSERT_TRANSACTIONS): {
        if (typeid(*data_structure) != typeid(SnapshotTransaction)) {
          cout << "Skipping experiment insert transactions for data structure " << ds_name << endl;
          continue;
        }
        // Master thread generates its own transaction after insertion happened.
        if (versioned_data_structure != nullptr) {
          tm.transactionCompleted(transaction);
        }
        run_insert_experiment_one_by_one(tm, versioned_data_structure, inserts, expected_edge_count_after_inserts);
        inserts_run = true;
        break;
      }
      case (DELETE): {
        if (run_on_raw_neighbourhood) {
          throw NotImplemented();
        }
        // Master thread generates its own transaction after insertion happened.
        if (versioned_data_structure == nullptr) {
          throw NotImplemented();
        }
        if (versioned_data_structure != nullptr) {
          tm.transactionCompleted(transaction);
        }
        run_delete_experiment(tm, versioned_data_structure, inserts, expected_edge_count_after_inserts);
        break;
      }
      case (GC): {
        if (versioned_data_structure == nullptr) {
          cout << "Skipping GC experiment for data structure " << ds_name << endl;
          continue;
        }
        tm.transactionCompleted(transaction);
        run_gc_experiment(tm, *versioned_data_structure, inserts_run, inserts);
        break;
      }
      case (STORAGE): {
        show_storage_sizes(ds_name, *data_structure);
        if (versioned_data_structure != nullptr) {
          tm.transactionCompleted(transaction);
        }
        break;
      }
      default:
        throw ConfigurationError("Unknown experiment type");
    }
    cout << endl;
  }

  if (data_structure != nullptr && typeid(*data_structure) != typeid(SnapshotTransaction)) {
    delete data_structure;
    data_structure = nullptr;
  }
  if (versioned_data_structure != nullptr) {
    delete versioned_data_structure;
    versioned_data_structure = nullptr;
  }
}


void Driver::run_analytics(Experiments ex, TopologyInterface &ds, bool run_on_raw_neighbourhood) {
  string experiment_name = Config::EXPERIMENT_MAPPING.find(ex)->second;
  cout << "Running " << experiment_name << " experiment ";
  cout.flush();

  vector<pair<vertex_id_t, uint64_t>> integral_values;
  vector<pair<vertex_id_t, double>> double_values;
  vector<pair<vertex_id_t, weight_t>> weight_values;

  vector<size_t> run_times;
  for (uint rep = 0; rep < config.repetitions; rep++) {
    auto start = chrono::steady_clock::now();
    switch (ex) {
      case (WCC): {
        integral_values = Algorithms::wcc(*this, ds, run_on_raw_neighbourhood);
        break;
      }
      case (SSSP): {
        weight_values = Algorithms::sssp(*this, ds, run_on_raw_neighbourhood);
        break;
      }
      case (LCC): {
        double_values = Algorithms::lcc(*this, ds, run_on_raw_neighbourhood);
        break;
      }
      case (CDLP): {
        integral_values = Algorithms::cdlp(*this, ds, run_on_raw_neighbourhood);
        break;
      }
      default:
        throw NotImplemented();
    }
    auto end = chrono::steady_clock::now();

    size_t microseconds = chrono::duration_cast<chrono::microseconds>(end - start).count();

    run_times.push_back(microseconds);
    reporter.add_repetition(ex, rep, microseconds);


    cout << ".";
    cout.flush();

#ifdef DEBUG
    switch (ex) {
      case (WCC): {
        // TODO this needs an check and bijektive translation, which is not implemented yet.
        break;
      }
      case (CDLP): {
        check_analytics<uint64_t>(ex, integral_values);
        break;

      }
      case (SSSP): {
        check_analytics<weight_t>(ex, weight_values);
        break;
      }
      case (LCC): {
        check_analytics<double>(ex, double_values);
        break;
      }
      default:
        throw NotImplemented();
    }
#endif
  }

  double average = (((double) sum(run_times)) / (double) run_times.size()) / 1000.0;
  cout << endl << experiment_name << " run in average in " << average << " milliseconds " <<
       endl;
}

void
Driver::run_bfs_experiment(TopologyInterface &ds, bool run_on_raw_neighbourhood, bool aquire_locks, bool after_inserts,
                           bool gapbs) {
  auto start_vertex = config.bfs_start_vertex();
  if (start_vertex == numeric_limits<vertex_id_t>::max()) {
    BFSSourceSelector ss(*this, config.base, ds);
    start_vertex = ss.get_source();
  }

  cout << "Running BFS experiment ";
  cout.flush();

  vector<pair<vertex_id_t, uint>> distances;

  vector<size_t> run_times;
  for (uint rep = 0; rep < config.repetitions; rep++) {
    // BFS
    auto start = chrono::steady_clock::now();
    distances = Algorithms::bfs(*this, ds, start_vertex, run_on_raw_neighbourhood, aquire_locks, gapbs);
    auto end = chrono::steady_clock::now();

    size_t microseconds = chrono::duration_cast<chrono::microseconds>(end - start).count();

    run_times.push_back(microseconds);
    reporter.add_repetition(BFS, rep, microseconds);


    cout << ".";
    cout.flush();

#ifdef DEBUG
    version_t version = after_inserts ? 1 : FIRST_VERSION;
    if (typeid(ds) == typeid(SnapshotTransaction)) {
      version = after_inserts ? dynamic_cast<SnapshotTransaction &>(ds).get_version() : FIRST_VERSION;
    }
    check_analytics(BFS, distances);
#endif
  }

  auto traversed_vertices = Algorithms::traversed_vertices(ds, distances);
  cout << "Traversed vertices " << traversed_vertices << " from " << ds.vertex_count() << " "
       << (float) traversed_vertices / (float) ds.vertex_count() * 100 << "%" << endl;
  double average = ((double) sum(run_times)) / (double) run_times.size() / 1000;
  cout << "BFS run in average in " << average << " milliseconds " << endl;
}

void Driver::load_base_dataset(TopologyInterface &ds, SortedCSRDataSource &base) {
  ds.bulkload(base);
}

void
run_inserts_in_transactions(bool weighted, size_t thread_id, TransactionManager &tm, EdgeList<weighted_edge_t> &el,
                            atomic_uint &insert_position,
                            VersionedTopologyInterface *ds, uint total_partitions, uint partition,
                            bool undirected) {
  tm.register_thread(thread_id);
  const uint batch_size = 3000;

  const uint total_work = el.edges.size();
  SnapshotTransaction tx = tm.getSnapshotTransaction(ds, true);
  while (insert_position.load() < total_work) {
    int work = insert_position.fetch_add(batch_size);
    int work_end = min(total_work, work + batch_size);

    while (work < work_end) {
      auto e = el.edges[work];
      tx.use_vertex_does_not_exists_semantics();
      tx.insert_vertex(e.src);
      tx.insert_vertex(e.dst);

      char weight[sizeof(e.weight)];
      memcpy((void *) &weight, (void *) &e.weight, sizeof(e.weight));
      if (undirected) {
        auto opposite = edge_t{e.dst, e.src};
        if (weighted) {
          tx.insert_edge(opposite, weight, sizeof(e.weight));
        } else {
          tx.insert_edge(opposite);
        }

      }
      if (weighted) {
        tx.insert_edge({e.src, e.dst}, weight, sizeof(e.weight));
      } else {
        tx.insert_edge({e.src, e.dst});
      }

      tx.execute();
      tm.transactionCompleted(tx);
      tm.getSnapshotTransaction(ds, true, tx);
      work++;
    }
  }
  tm.transactionCompleted(tx);

//  cout << "total" << total_partitions << endl;
//  cout << "Partition: " << partition << endl;
//    SnapshotTransaction tx = tm.getSnapshotTransaction(ds, thread_id);
//  for (const auto& e : el) {
//    if (e.src % total_partitions == partition) {
//      tx.insert_edge(e);
//      tx.execute();
//      tm.transactionCompleted(tx, thread_id);
//      tm.getSnapshotTransaction(ds, thread_id, tx);
//    }
//  }
  tm.deregister_thread(thread_id);
}

void Driver::run_insert_experiment_one_by_one(TransactionManager &tm, VersionedTopologyInterface *ds,
                                              EdgeList<weighted_edge_t> &el,
                                              size_t expected_edge_count) {
  cout << "Running insert experiment inserting " << el.edges.size() << " edges." << endl;
  uint threads = config.insert_threads;

  auto start = chrono::steady_clock::now();
  if (threads == 1) {  // TODO remove this if
    SnapshotTransaction tx = tm.getSnapshotTransaction(ds, true);

    for (auto e : el.edges) {
      tx.use_vertex_does_not_exists_semantics();
      tx.insert_vertex(e.src);
      tx.insert_vertex(e.dst);

//      char weight[sizeof(e.weight)];
//      memcpy((void*) &weight, (void*) &e.weight, sizeof(e.weight));
      if (config.undirected) {
        auto opposite = edge_t{e.dst, e.src};
        if (config.weighted) {
          tx.insert_edge(opposite, (char *) &e.weight, sizeof(e.weight));
        } else {
          tx.insert_edge(opposite);
        }
      }

      if (config.weighted) {
        tx.insert_edge({e.src, e.dst}, (char *) &e.weight, sizeof(e.weight));
      } else {
        tx.insert_edge({e.src, e.dst});
      }
      tx.execute();
      tm.transactionCompleted(tx);
      tm.getSnapshotTransaction(ds, true, tx);
    }
    tm.transactionCompleted(tx);
  } else {
    atomic<uint> insert_index(0);
    vector<thread> ts;
    uint partition = 0;
    for (uint i = 1; i < threads + 1; i++) {
      ts.emplace_back(run_inserts_in_transactions, config.weighted, i, ref(tm), ref(el), ref(insert_index), ds,
                      config.insert_threads,
                      partition, config.undirected);
      partition++;
    }

    for (auto &t : ts) {
      t.join();
    }
  }
  auto end = chrono::steady_clock::now();

  size_t microseconds = chrono::duration_cast<chrono::microseconds>(end - start).count();
  reporter.add_repetition(INSERT_TRANSACTIONS, 0, microseconds);

  cout << "Inserting took: " << microseconds / 1000 << " milliseconds " << endl;
  cout << "This is " << ((float) el.edges.size() / ((float) microseconds / 1000000.0)) << " edges per second" << endl;
#if defined(DEBUG) && CHECKINSERT
  auto tx = tm.getSnapshotTransaction(ds, false);
  check_insert(tx, el, expected_edge_count);
  tm.transactionCompleted(tx);
#endif
  cout << "checked" << endl;
}


void
run_deletes_in_transactions(size_t thread_id, TransactionManager &tm, EdgeList<weighted_edge_t> &el,
                            atomic_uint &deletion_position,
                            VersionedTopologyInterface *ds, uint total_partitions, uint partition,
                            bool undirected) {
  tm.register_thread(thread_id);
  const uint batch_size = 3000;

  const uint total_work = el.edges.size();
  SnapshotTransaction tx = tm.getSnapshotTransaction(ds, true);
  while (deletion_position.load() < total_work) {
    int work = deletion_position.fetch_add(batch_size);
    int work_end = min(total_work, work + batch_size);

    while (work < work_end) {
      auto e = el.edges[work];
      edge_t edge(e.src, e.dst);

      if (undirected) {
        auto opposite = edge_t{e.dst, e.src};
        tx.delete_edge(opposite);
      }
      tx.delete_edge(edge);
      tx.execute();
      tm.transactionCompleted(tx);
      tm.getSnapshotTransaction(ds, true, tx);
      work++;
    }
  }
  tm.transactionCompleted(tx);
  tm.deregister_thread(thread_id);
}


void Driver::run_delete_experiment(TransactionManager &tm, VersionedTopologyInterface *ds,
                                   EdgeList<weighted_edge_t> &existing_edges, size_t exptected_edge_count) {
  auto to_delete = generate_deletions(existing_edges, 0.1);

  cout << "Running delete experiment deleting " << to_delete.edges.size() << " edges." << endl;
  uint threads = config.insert_threads;

  auto start = chrono::steady_clock::now();
  atomic<uint> deletion_index(0);
  vector<thread> ts;
  uint partition = 0;
  for (uint i = 1; i < threads + 1; i++) {
    ts.emplace_back(run_deletes_in_transactions, i, ref(tm), ref(to_delete), ref(deletion_index), ds,
                    config.insert_threads,  // TODO rename to writer threads
                    partition, config.undirected);
    partition++;
  }

  for (auto &t : ts) {
    t.join();
  }
  auto end = chrono::steady_clock::now();

  size_t microseconds = chrono::duration_cast<chrono::microseconds>(end - start).count();
  reporter.add_repetition(DELETE, 0, microseconds);

  cout << "Deleting took: " << microseconds / 1000 << " milliseconds " << endl;
  cout << "This is " << ((float) to_delete.edges.size() / ((float) microseconds / 1000000.0)) << " edges per second"
       << endl;
#if defined(DEBUG) && CHECKINSERT
  cout << "Checking deletions" << endl;
  auto tx = tm.getSnapshotTransaction(ds, false);
  check_deletions(tx, to_delete);
  tm.transactionCompleted(tx);
#endif

  // Reinsert the edges so we can run further experiments and use the same gold standards.
  run_insert_experiment_one_by_one(tm, ds, to_delete, exptected_edge_count);
}

EdgeList<weighted_edge_t> Driver::read_insert_dataset() {
  EdgeList<weighted_edge_t> edge_list;
  if (config.weighted_graph_source) {
    edge_list.read_from_binary_file(config.insertions.path);
  } else {
    EdgeList<edge_t> t;
    t.read_from_binary_file(config.insertions.path);
    edge_list = t.add_weights(config.weighted);
  }
  return edge_list;
}


SortedCSRDataSource Driver::read_base_dataset() {
  SortedCSRDataSource out;
//  out.read_from_binary_file(config.base.path);
  // TODO reactivate
  return out;
}

void Driver::check_insert(TopologyInterface &ds, EdgeList<weighted_edge_t> &el, size_t expected_edge_count) {
  cout << "Validating insert experiment" << endl;
  auto edge_count = ds.edge_count();
  assert(edge_count == expected_edge_count);

  cout << "Checking if all edges exist." << endl;
#pragma omp parallel for
  for (auto i = 0u; i < el.edges.size(); i++) {
    auto e = el.edges[i];

    if (config.weighted && typeid(ds) == typeid(SnapshotTransaction &)) {
      auto &tx = dynamic_cast<SnapshotTransaction &>(ds);
      weight_t w1;
      auto e1 = tx.get_weight({e.src, e.dst}, (char *) &w1);
      assert(e1);
      assert(w1 == e.weight);

      if (config.undirected) {
        weight_t w2;
        auto e2 = tx.get_weight({e.dst, e.src}, (char *) &w2);
        assert(e2);
        assert(w2 == e.weight);
      }
    } else {
      assert(ds.has_edge({e.src, e.dst}));
      if (config.undirected) {
        edge_t opposite = {e.dst, e.src};
        assert(ds.has_edge(opposite));
      }
    }
//    if (i % 1000 == 0) {
//      cout << ".";
//    }

  }

  // Check properties
  if (config.weighted && typeid(ds) == typeid(SnapshotTransaction &)) {
    if (typeid(weight_t) != typeid(dst_t)) {
      cerr << "Weight type needs to be dst_t to check property iterator" << endl;
    } else if (config.weighted_graph_source) {
      cerr << "Weights are given externally, cannot check them via property iterator." << endl;
    } else {
      cout << "Checking properties" << endl;

      auto tx = dynamic_cast<SnapshotTransaction &>(ds);

#pragma omp parallel for
      for (vertex_id_t v = 0; v < ds.max_physical_vertex(); v++) {
        if (ds.has_vertex_p(v)) {
          auto l_v = ds.logical_id(v);

          SORTLEDTON_WITH_PROPERTIES_ITERATE(tx, v, {
            if (p != l_v) {
              auto l_d = ds.logical_id(e);
              assert(l_d == p);
            }
          });
        }
      }
    }
  }


  const string gold_standard_file_sizes =
          config.gold_standard_directory + "/insert_adjacency_set_sizes_" + config.base.get_name() +
          (config.undirected ? "_undirected" : "") + ".goldStandard";
  if (!file_exists(gold_standard_file_sizes)) {
    cout << "Writing new gold standard for: " << gold_standard_file_sizes << endl;
    size_t size = ds.vertex_count();
    vector<pair<vertex_id_t, size_t>> sizes;


    for (uint v = 0; v < ds.max_physical_vertex(); v++) {
      if (ds.has_vertex_p(v)) {
        vertex_id_t logical_vertex = ds.logical_id(v);
        size_t neighbourhood_size = ds.neighbourhood_size_p(v);
        sizes.emplace_back(logical_vertex, neighbourhood_size);
      }
    }

    sort(sizes.begin(), sizes.end());

    ofstream f(gold_standard_file_sizes, ofstream::binary | ofstream::out);
    if (!f.good()) {
      assert(false);
    }

    f.write((char *) &size, sizeof(size));

    for (auto &t : sizes) {
      f.write((char *) &(t.first), sizeof(t.first));
      f.write((char *) &(t.second), sizeof(t.second));
    }


    f.close();
  } else {
    vector<pair<vertex_id_t, size_t>> actual_sizes;

    for (uint v = 0; v < ds.max_physical_vertex(); v++) {
      if (ds.has_vertex_p(v)) {
        vertex_id_t logical_vertex = ds.logical_id(v);
        size_t neighbourhood_size = ds.neighbourhood_size_p(v);
        actual_sizes.emplace_back(logical_vertex, neighbourhood_size);
      }
    }
    sort(actual_sizes.begin(), actual_sizes.end());

    ifstream f(gold_standard_file_sizes, ifstream::in | ifstream::binary);

    size_t size;
    f.read((char *) &size, sizeof(size));

    assert(size == actual_sizes.size());

    vertex_id_t id;
    size_t neighbourhood_size;
    for (uint i = 0; i < size; i++) {
      f.read((char *) &id, sizeof(id));
      f.read((char *) &neighbourhood_size, sizeof(neighbourhood_size));
      auto actual_size = actual_sizes[i];
      assert(id == actual_size.first);
      assert(neighbourhood_size == actual_size.second);
    }

    f.close();
  }

  vertex_id_t start_vertex = config.bfs_start_vertex();
  if (start_vertex == numeric_limits<vertex_id_t>::max()) {
    BFSSourceSelector ss(*this, config.base, ds);
    start_vertex = ss.get_source();
  }

  vector<pair<vertex_id_t, uint>> distances;
  if (typeid(ds) == typeid(SnapshotTransaction)) {
    distances = Algorithms::bfs(*this, ds, start_vertex, false, false, true);
  } else {
    distances = Algorithms::bfs(*this, ds, start_vertex);
  }
  check_analytics(BFS, distances);
}

void Driver::run_page_rank_experiment(TopologyInterface &ds, bool run_on_raw_neighbourhood, bool gapbs) {
  cout << "Running PR experiment ";
  cout.flush();

  vector<pair<vertex_id_t, double>> scores;

  vector<size_t> run_times;
  for (uint rep = 0; rep < config.repetitions; rep++) {
    auto start = chrono::steady_clock::now();
    scores = Algorithms::page_rank(*this, ds, run_on_raw_neighbourhood, gapbs);
    auto end = chrono::steady_clock::now();

    size_t microseconds = chrono::duration_cast<chrono::microseconds>(end - start).count();

    run_times.push_back(microseconds);
    reporter.add_repetition(PR, rep, microseconds);


    cout << ".";
    cout.flush();

#ifdef DEBUG
    check_analytics(PR, scores);
#endif
  }

  double average = ((double) sum(run_times)) / (double) run_times.size() / 1000;
  cout << endl << "PR run in average in " << average << " milliseconds " << endl;
}

void Driver::show_storage_sizes(string ds_name, TopologyInterface &ds) {
  cout << "Storage size of " << setw(10) << ds_name << endl;
  ds.report_storage_size();
}

void
Driver::run_gc_experiment(TransactionManager &tm, VersionedTopologyInterface &ds, bool inserts_run,
                          EdgeList<weighted_edge_t> &inserts) {
  cout << "Running GC experiment " << endl;

  tm.update_min_version();
  auto start = chrono::steady_clock::now();
  ds.gc_all();
  auto end = chrono::steady_clock::now();

  size_t milliseconds = chrono::duration_cast<chrono::milliseconds>(end - start).count();
  reporter.add_repetition(GC, 0, milliseconds);

  cout << ".";
  cout.flush();

#if defined(DEBUG) && CHECKINSERT
  if (inserts_run) {
    auto tx = tm.getSnapshotTransaction(&ds, false);
    check_insert(tx, inserts, config.undirected ? inserts.edges.size() * 2 : inserts.edges.size());
    tm.transactionCompleted(tx);
  }
  check_gc_experiment(ds);
#endif

  cout << endl << "GC run in " << milliseconds << " milliseconds " << endl;
  cout << "Collected " << dynamic_cast<VersioningBlockedSkipListAdjacencyList &>(ds).gced_edges << " edge versions"
       << endl;
  cout << "Merged " << dynamic_cast<VersioningBlockedSkipListAdjacencyList &>(ds).gc_merges << " skip list blocks"
       << endl;
  cout << "Changed  " << dynamic_cast<VersioningBlockedSkipListAdjacencyList &>(ds).gc_to_single_block
       << " skip list blocks to single blocks" << endl;
}

void Driver::check_gc_experiment(VersionedTopologyInterface &ds) {
}

vertex_id_t Driver::sssp_start_vertex(TopologyInterface &ds) {
  auto v = config.sssp_start_vertex();
  if (v == numeric_limits<vertex_id_t>::max()) {
    BFSSourceSelector ss(*this, config.base, ds);
    return ss.get_source();
  }
  return v;
}

EdgeList<weighted_edge_t>
Driver::generate_deletions(EdgeList<weighted_edge_t> &existing_edges, double deletion_percentage) {
  EdgeList<weighted_edge_t> to_delete;
  to_delete.edges.reserve(existing_edges.edges.size() * deletion_percentage);

  auto rng = std::default_random_engine{};
  shuffle(existing_edges.edges.begin(), existing_edges.edges.end(), rng);

  for (auto i = 0; i < existing_edges.edges.size() * deletion_percentage; i++) {
    to_delete.edges.push_back(existing_edges.edges[i]);
  }
  return to_delete;
}

void Driver::check_deletions(TopologyInterface &ds, EdgeList<weighted_edge_t> &el) {
  for (const auto &e : el) {
    assert(!ds.has_edge(edge_t{e.src, e.dst}));
    if (config.undirected) {
      edge_t opposite = {e.dst, e.src};
      assert(!ds.has_edge(opposite));
    }
  }
   // TODO check sizes after deletions
}

