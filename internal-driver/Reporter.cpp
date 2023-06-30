//
// Created by per on 02.09.20.
//

#include <sstream>
#include <algorithm>
#include <cassert>

#include <utils/NotImplemented.h>
#include "Reporter.h"

void Reporter::add_repetition(Experiments experiment, int repetition,
                              unordered_map<string, ulong> metrics) {
  assert(!dataset.name.empty() && "Dataset name not set.");

  string s_experiment = Config::EXPERIMENT_MAPPING.find(experiment)->second;
  string s_data_structure = Config::DATA_STRUCTURE_MAPPING.find(data_structure)->second;
  s_data_structure += "(" + string_join(",", data_structure_parameters) + ")";

  stringstream line;
  line << execution_id << separator;
  line << time(nullptr) << separator;
  line << repetition << separator;
  line << dataset.name << separator;
  line << s_experiment << separator;
  line << s_data_structure << separator;


  for (const auto& m : metrics) {
    if (find(header.begin(), header.end(), m.first) == header.end()) {
      throw NotImplemented("Cannot add new headers to report file yet.");
    }
  }

  for (uint i = 0; i < header.size(); i++) {
    if (i < 6) {  // do not write anything for the first 5 headers.
      continue;
    }
    auto h = header[i];
    auto element = metrics.find(h);
    if (metrics.end() != element) {
      line << element->second;
    }
    line << separator;
  }

  file << line.str() << endl;
}

ulong Reporter::get_highest_execution_id() {
  file.seekg(0);

  auto line_number = 0;
  string last_line;
  string line;
  while(1) {
    last_line = line;
    getline(file, line);
    if (file.eof()) { // Needs to be tested here because, eof and no character read also sets fail bit.
      break;
    }
    if (file.fail()) {
      throw ConfigurationError("Problems reading the highest execution id.");
    }
    line_number++;
  }
  file.clear();

  if (line_number == 1) {
    return 0;
  } else {
    size_t pos = last_line.find(separator);
    string s = last_line.substr(0, pos);
    return stoi(s);
  }
}

vector<string> Reporter::get_header() {
  file.seekg(0);

  vector<string> v_header;

  string s_header;
  getline(file, s_header);

  int last_pos = -1;
  size_t pos = s_header.find(separator);
  while (pos != string::npos) {
    v_header.push_back(s_header.substr(last_pos + 1, pos - (last_pos + 1)));
    last_pos = pos;
    pos = s_header.find(separator, last_pos + 1);
  }

  return v_header;
}

void Reporter::write_standard_header() {
  stringstream ss;

  for (const auto& h : standard_header) {
    ss << h << separator;
  }

  file << ss.str() << endl;
  file.flush();
}

void Reporter::set_dataset(Dataset dataset) {
  this->dataset = dataset;
}

void Reporter::set_data_structure(DataStructures ds, const vector<string>& parameters) {
  this->data_structure = ds;
  this->data_structure_parameters = parameters;
}

void Reporter::add_repetition(Experiments experiment, int repetition, ulong runtime) {
  unordered_map<string, ulong> metrics {{"runtime", runtime}};
  add_repetition(experiment, repetition, metrics);
}
