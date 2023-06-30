//
// Created by per on 31.08.20.
//

#include "Configuration.h"

#include <getopt.h>
#include <iostream>

#include "utils/utils.h"

void Config::initialize(int argc, char **argv) {
  int c;

  while (1) {
    int option_index = 0;
    static struct option long_options[] = {
            {"experiments", required_argument, 0, 'e'},
            {"data_structures", required_argument, 0, 's'},
            {"dataset_base", required_argument, 0, 'b'},
            {"dataset_insert", required_argument, 0, 'i'},
            {"dataset_delete", required_argument, 0, 'd'},
            {"repetitions", required_argument, 0, 'r'},
            {"release_run", no_argument, 0, 'l'},
            {"prefetch_blocks", required_argument, 0, 'p'},
            {"undirected", no_argument, 0, 'u'},
            {"insert_threads", required_argument, 0, 't'},
            {"omp_threads", required_argument, 0, 'o'},
            {"weighted", no_argument, 0, 'w'},
            {"analytics", required_argument, 0, 'a'},
    };

    c = getopt_long(argc, argv, "",
                    long_options, &option_index);
    if (c == -1)
      break;

    switch (c) {
      case 'a':
        graphalytics = GraphalyticsProperties(optarg);
        break;
      case 't':
        insert_threads = stoi(optarg);
        break;
      case 'p':
        prefetch_blocks = stoi(optarg);
        break;
      case 'o':
        omp_threads = stoi(optarg);
        break;
      case 'l':
        release = true;
        break;
      case 'e':
        experiments = parse_experiments(optarg);
        for (auto e : experiments) {
          experiment_set.insert(e.first);
        }
        break;
      case 's':
        data_structures = parse_data_structures(optarg);
        break;
      case 'b':
        base = Dataset(optarg, CSR_SRC);
        break;
      case 'w':
        weighted = true;
        break;
      case 'i':
        insertions = Dataset(optarg, EDGE_LIST);
        break;
      case 'd':
        deletions = Dataset(optarg, EDGE_LIST);
        break;
      case 'r':
        repetitions = stoi(optarg);
        break;
      case 'u':
        undirected = true;
        break;
      case '?':
        printf("No help provided read src.\n");
        break;
      default:
        printf("?? getopt returned character code 0%o ??\n", c);
    }
  }

  if (optind < argc) {
    throw ConfigurationError("Unknown positional argument.");
  }

  if (!graphalytics.is_empty()) {
    if (!undirected) {
      throw ConfigurationError("Graphalytics is only supported on undirected graphs.");
    }

    for (auto e : experiment_set) {
      if (graphalytics.is_graphalytic_experiment(e) && !graphalytics.supports_experiment(e)) {
        throw ConfigurationError("Graphalytics is used and graph does not support the following experiment: " + e);
      }
    }

    insertions = Dataset(graphalytics.insertion_set(), EDGE_LIST);
    cout << "Using insertion dataset: " + insertions.path;
    base = Dataset(graphalytics.base_graph(), CSR_SRC);
    cout << "Using base dataset: " + base.path;

    weighted_graph_source = true;
  }
}

vector <string> Config::string_split(char seperator, string list) {
  char delim = seperator;
  std::size_t current, previous = 0;
  vector<string> cont;
  current = list.find(delim);
  while (current != std::string::npos) {
    cont.push_back(list.substr(previous, current - previous));
    previous = current + 1;
    current = list.find(delim, previous);
  }
  cont.push_back(list.substr(previous, current - previous));
  return cont;
}

vector<pair<DataStructures, vector<string>>> Config::parse_data_structures(string arg) {
  vector<pair<DataStructures, vector<string>>> ret;
  auto ds = string_split(',', arg);

  auto ds_map = reverse_map(DATA_STRUCTURE_MAPPING);
  for (const auto& d : ds) {
    auto data_structure_name = d;

    vector<string> parameters;
    auto parameters_start = d.find('(');

    if (parameters_start != string::npos) {
        data_structure_name = d.substr(0, parameters_start);
        parameters = string_split('\'', d.substr(parameters_start + 1, d.find(')') - (parameters_start + 1)));
    }

    auto mapping = ds_map.find(data_structure_name);
    if (mapping == ds_map.end()) {
      throw ConfigurationError("Unknown data structure " + d);
    } else {
      ret.emplace_back(mapping->second, parameters);
    }
  }

  return ret;
}

vector<pair<Experiments, vector<string>>> Config::parse_experiments(string arg) {
  vector<pair<Experiments, vector<string>>> ret;
  auto es = string_split(',', arg);

  auto map = reverse_map(EXPERIMENT_MAPPING);
  for (const auto& e : es) {
    vector<string> parameters;
    auto experiment_name = e;
    auto parameters_start = e.find('(');

    if (parameters_start != string::npos) {
      experiment_name = e.substr(0, parameters_start);
      parameters = string_split('\'', e.substr(parameters_start + 1, e.find(')') - (parameters_start + 1)));
    }

    auto mapping = map.find(experiment_name);
    if (mapping == map.end()) {
      throw ConfigurationError("Unknown experiment " + e);
    } else {
      ret.emplace_back(mapping->second, parameters);
    }
  }

  return ret;
}

const unordered_map<DataStructures, string> Config::DATA_STRUCTURE_MAPPING {
  {VERSIONED, "v"}
};

const unordered_map<Experiments, string> Config::EXPERIMENT_MAPPING{
        {INSERT_TRANSACTIONS, "insert_tx"},
        {DELETE, "delete"},
        {BFS,    "bfs"},
        {PR, "pr"},
        {STORAGE, "storage"},
        {GC, "gc"},
        {GAPBS_BFS, "bsbfs"},
        {GAPBS_PR, "bspr"},
        {SSSP, "sssp"},
        {WCC, "wcc"},
        {LCC, "lcc"},
        {CDLP, "cdlp"},
};

const string Config::gold_standard_directory = "/space/fuchs/shared/graph_two_gold_standards";

double Config::page_rank_error() {
    return PAGE_RANK_ERROR;
}

double Config::page_rank_damping_factor() {
  if (graphalytics.is_empty()) {
    return PAGE_RANK_DAMPING_FACTOR;
  } else {
    return graphalytics.page_rank_damping_factor;
  }
}

int Config::page_rank_max_iterations() {
  if (graphalytics.is_empty()) {
    return PAGE_RANK_ITERATIONS;
  } else {
    return graphalytics.page_rank_num_iterations;
  }
}

int Config::cdlp_max_iterations() {
  if (graphalytics.is_empty()) {
    return CDLP_MAX_ITERATIONS;
  } else {
    return graphalytics.cdlp_max_iterations;
  }
}

double Config::sssp_delta() {
  return SSSP_DELTA;
}

vertex_id_t Config::sssp_start_vertex() {
  if (graphalytics.is_empty()) {
    return numeric_limits<vertex_id_t>::max();
  } else {
    return graphalytics.sssp_source_vertex;
  }
}

vertex_id_t Config::bfs_start_vertex() {
  // TODO make graphalytics option
  if (graphalytics.is_empty()) {
    return numeric_limits<vertex_id_t>::max();
  } else {
    return graphalytics.bfs_source_vertex;
  }
}

// TODO clean up for a common interface and to use for all gold standards
string Config::gold_standard(Experiments e) {
  auto experiment_name = Config::EXPERIMENT_MAPPING.find(e)->second;
  if (graphalytics.is_empty()) {
    return gold_standard_directory + "/" + experiment_name + "_" + base.get_name() + (undirected ? "_undirected" : "") + ".goldStandard";;
  } else {
    return graphalytics.gold_standard(experiment_name);
  }
}
