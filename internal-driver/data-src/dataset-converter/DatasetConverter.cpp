//
// Created by per on 01.09.20.
//

#include <fstream>
#include <iostream>
#include <random>
#include <vector>
#include <unordered_map>
#include <limits>
#include <algorithm>
#include <unordered_set>

#include "DatasetConverter.h"
#include "data-structure/data_types.h"
#include "internal-driver/data-src/SortedCSRDataSource.h"
#include "Options.h"

DatasetConverter::DatasetConverter(int argc, char **argv) {
  o = parseOptions(argc, argv);
}


void show_max_degree(const vector<temporal_edge_t>& edge_list, size_t vertex_count) {
  unordered_map<vertex_id_t, size_t> counter;
  counter.reserve(vertex_count);

  size_t m = 0;
  for (auto e : edge_list) {
    auto c = counter.find(e.src);
    if (c != counter.end()) {
      c->second++;
      m = std::max(m, c->second);
    } else {
      counter.insert({e.src, 1});
    }
  }

  cout << "Max degree: " << m << endl;
}

void DatasetConverter::run() {
  if (o.input_format == WEIGHTED_EDGELIST_TEXT) {
    return templated_run<weighted_edge_t>();
  } else if (o.input_format == TEMPORAL_EDGELIST_TEXT) {
    return templated_run<temporal_edge_t>();
  } else {
    return templated_run<edge_t>();
  }
}

bool DatasetConverter::is_comment(const string &line) {
  char commentChars[] = {'#', '%'};

  for (char c : commentChars) {
    if (line.find(c) == 0) {
      return true;
    }
  }
  return false;
}

char DatasetConverter::detect_seperator(const string &input) {
  ifstream in;
  in.exceptions(fstream::failbit | fstream::badbit);

  in.open(input);

  const char seperators[] = {',', ' ', '\t'};

  string line;
  while (getline(in, line)) {
    if (is_comment(line)) {
      continue;
    } else {
      for (char s : seperators) {
        if (line.find(s) != string::npos) {
          return s;
        }
      }
      cout << "Couldn't detect seperator in line: " << line << endl;
      exit(Options::BAD_FORMAT);
    }
  }
  cout << "Couldn't detect seperator in line: " << line << endl;
  exit(Options::BAD_FORMAT);
}

void DatasetConverter::write_base_dataset(SortedCSRDataSource csr) {
  cout << "Writing CSR to " << o.output_path + o.base_file_name << endl;
  ofstream f{o.output_path + o.base_file_name, ofstream::out | ofstream::binary};

  SortedCSRDataSource::FileHeader header;
  header.vertex_count = csr.vertex_count();
  header.edge_count = csr.adjacency_lists.size();

  f.write((char *) &header, sizeof(header));
  f.write((char *) &csr.adjacency_index[0], sizeof(size_t) * (header.vertex_count + 1));
  f.write((char *) &csr.adjacency_lists[0], sizeof(dst_t) * (header.edge_count));

  f.close();

  SortedCSRDataSource rr;
  const string s = o.output_path + o.base_file_name;
  rr.read_from_binary_file(s);
}

void DatasetConverter::write_degree_information(SortedCSRDataSource &graph) {
  string file_path = o.output_path + o.base_file_name + ".degrees";
  cout << "Writing degree information to " << file_path << endl;
  ofstream o(file_path, ofstream::out | ofstream::binary);

  size_t vc = graph.vertex_count();
  o.write((char *) &vc, sizeof(vc));

  size_t m = 0;
  for (vertex_id_t v = 0; v < graph.vertex_count(); v++) {
    size_t d = graph.adjacency_index[v + 1] - graph.adjacency_index[v];
    o.write((char *) &v, sizeof(v));
    o.write((char *) &d, sizeof(d));
    m = max(m, d);
  }

  cout << "max degree according to csr: " << m << endl;

  o.close();
}