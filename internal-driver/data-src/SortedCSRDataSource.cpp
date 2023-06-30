//
// Created by per on 31.08.20.
//

#include <iostream>
#include <fstream>
#include "SortedCSRDataSource.h"
#include "IOException.h"

void SortedCSRDataSource::read_from_binary_file(const string &path) {
  ifstream f {path, ifstream::in | ifstream::binary};

  FileHeader header;
  f.read((char*) &header, sizeof(header));
  if (!f.good()) {
    throw IOException("Could not read header of CSR file.");
  }

  adjacency_index.clear();
  adjacency_index.resize(header.vertex_count + 1);

  adjacency_lists.clear();
  adjacency_lists.resize(header.edge_count);

  FileBody body{ adjacency_index.data(),  adjacency_lists.data()};

  f.read((char*) body.offsets, (header.vertex_count + 1) * sizeof(size_t));
  f.read((char*) body.adjacency_lists, header.edge_count * sizeof(dst_t));

  if (!f.good()) {
    throw IOException("Could not read body of CSR file.");
  }

  char eof_test;
  f.read(&eof_test, 1);

  if (!f.eof()) {
    throw IOException("File had additional content.");
  }

  f.close();
}

unordered_set<dst_t> SortedCSRDataSource::get_neighbour_set(vertex_id_t v) {
  auto size = adjacency_index[v+1] - adjacency_index[v];
  unordered_set<dst_t> ns;
  ns.reserve(size);

  for (auto i = adjacency_index[v]; i < adjacency_index[v + 1]; i++) {
    ns.insert(adjacency_lists[i]);
  }

  return ns;
}
