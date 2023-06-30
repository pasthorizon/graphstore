//
// Created by per on 30.03.21.
//

#include <fstream>
#include <iostream>

#include <utils/utils.h>
#include "GraphalyticsProperties.h"
#include "Configuration.h"

const string GraphalyticsProperties::PROPERTY_FILE_EXTENSION = ".properties";

const unordered_map<string, Experiments> GraphalyticsProperties::algorithm_to_experiment{
        {"bfs", GAPBS_BFS},
        {"pr", GAPBS_PR},
        {"cdlp", CDLP},
        {"lcc", LCC},
        {"sssp", SSSP},
        {"wcc", WCC}
};

GraphalyticsProperties::GraphalyticsProperties() : empty(true) {

}

GraphalyticsProperties::GraphalyticsProperties(path property_file) : empty(false) {
  if (!exists(property_file)) {
    throw ConfigurationError("Graphalytics Property file does not exist,");
  }

  base_dir = property_file.parent_path();
  graph_name = property_file.filename().replace_extension(path());
  parse(property_file);
}

void GraphalyticsProperties::parse(const path& property_file) {
  if (property_file.extension().generic_string().compare(PROPERTY_FILE_EXTENSION) != 0) {
    throw ConfigurationError("Graphalytics file path is not a property file.");
  }

  ifstream f(property_file.generic_string());

  string line;
  while(getline(f, line)) {
    if (!is_comment(line) && !line.empty()) {
      auto key_value = split(line, '=');
      auto key = key_value[0];
      auto value = key_value[1];
      trim(key);
      trim(value);

      vector<string> key_parts = split(key, '.');

      auto last_key_part = *(key_parts.end() - 1);
      auto last_two_key_parts = *(key_parts.end() - 2) + "." + *(key_parts.end() - 1);
      if (last_key_part == "algorithms") {
        parse_algorithms(value);
      } else if (last_key_part.compare("vertices") == 0) {
          vertices = stoi(value);
      } else if (last_key_part.compare("edges") == 0) {
          edges = stoi(value);
      } else if (last_key_part.compare("directed") == 0) {
          if (value.compare("true") == 0) {
            throw ConfigurationError("Cannot process directed Graphalytics graphs");
          } else if (value.compare("false") == 0) {
            // NOP
          } else {
            throw ConfigurationError("Unknown value for Graphalytics property directed: " + value);
          }
      } else if (last_two_key_parts.compare("edge-properties.names") == 0) {
        if (value.compare("weight") != 0) {
          throw ConfigurationError("Unknown Graphalytic edge property name.");
        }
      } else if (last_two_key_parts.compare("edge-properties.types") == 0) {
        if (value.compare("real") != 0) {
          throw ConfigurationError("Unknown Graphalytic edge property type");
        }
      } else if (last_two_key_parts.compare("bfs.source-vertex") == 0) {
        bfs_source_vertex = stoi(value);
      }  else if (last_two_key_parts.compare("cdlp.max-iterations") == 0) {
        cdlp_max_iterations = stoi(value);
      } else if (last_two_key_parts.compare("pr.damping-factor") == 0) {
        page_rank_damping_factor = stod(value);
      } else if (last_two_key_parts.compare("pr.num-iterations") == 0) {
        page_rank_num_iterations = stoi(value);
      } else if (last_two_key_parts.compare("sssp.source-vertex") == 0) {
        sssp_source_vertex = stoi(value);
      } else {
#ifdef DEBUG
        cout << "Did not parse unknown Graphalytics key: " << key;
#endif
      }
    }
  }
}

// From: https://www.cplusplus.com/articles/2wA0RXSz/
vector<string> GraphalyticsProperties::split(const string& s, const char& c) {
  string buff{""};
  vector<string> v;

  for(auto n : s)
  {
    if(n != c) {
      buff+=n;
    } else {
      if(n == c && buff != "") {
        v.push_back(buff);
        buff = "";
      }
    }
  }
  if(buff != "") v.push_back(buff);

  return v;
}

bool GraphalyticsProperties::is_comment(const string& line) {
  return line.rfind('#') == 0; // Starts with a #
}

void GraphalyticsProperties::parse_algorithms(const string& algorithm_list) {
  auto algorithms = split(algorithm_list, ',');

  for (auto a : algorithms) {
    trim(a);
    auto e = algorithm_to_experiment.find(a);
    if (e != algorithm_to_experiment.end()) {
      experiments.insert(e->second);
      if (e->second == GAPBS_BFS) {
        experiments.insert(BFS);
      }
      if (e->second == GAPBS_PR) {
        experiments.insert(PR);
      }
    }
  }
}

bool GraphalyticsProperties::is_empty() {
  return empty;
}

bool GraphalyticsProperties::supports_experiment(Experiments e) {
  return experiments.find(e) != experiments.end();
}

string GraphalyticsProperties::base_graph() {
  return base_dir / "base.csr";

}

string GraphalyticsProperties::insertion_set() {
  return base_dir / "insertions.edgeList";
}

string GraphalyticsProperties::gold_standard(string e) {
  return base_dir / (e + ".gold_standard");
}

bool GraphalyticsProperties::is_graphalytic_experiment(Experiments e) {
  return e == WCC || e == LCC || e == CDLP || e == PR || e == GAPBS_PR || e == BFS || e == GAPBS_BFS;
}


