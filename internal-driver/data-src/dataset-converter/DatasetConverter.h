//
// Created by per on 01.09.20.
//

#ifndef LIVE_GRAPH_TWO_DATASETCONVERTER_H
#define LIVE_GRAPH_TWO_DATASETCONVERTER_H


#include <fstream>
#include <iostream>
#include <random>
#include <vector>
#include <unordered_map>
#include <limits>
#include <algorithm>
#include <unordered_set>

#include "Options.h"
#include "data-structure/data_types.h"
#include "internal-driver/data-src/SortedCSRDataSource.h"

using namespace std;

/**
 * Generate binary graph representations from text files.
 *
 * Can parse csv files as found on konect.cc.
 * Can parse files with edge creation timestamp.
 * Cannot parse files with creations and deletions yet, e.g. wikipedia hyperlinks.
 *
 * Insertions are sampled randomly and output is shuffled if the network is parsed as none temporal.
 * If the network is parsed as temporal network, insertions are chosen to be the last x% percent of edges and are
 * inserted according to their insertion timestamp.
 * Deletetions are sampled randomly and output is shuffled.
 *
 * Deletions and Insertion sets are none overlapping.
 */
class DatasetConverter {
public:
    DatasetConverter(int argc, char **argv);

    void run();

private:
    Options o;

    char detect_seperator(const string &path);

    bool is_comment(const string &line);

    template<typename E>
    void templated_run() {
      cout << "Parsing text file. Densify: " << o.densify << "Edge type " << typeid(E).name() << " Temporal value: "
           << o.temporal_value_position << endl;
      size_t vertex_count;

      vector<E> edge_list = parse_text_file<E>(o, vertex_count);

      edge_list = clean_data<E>(edge_list, o.make_undirected);

      size_t deletion_set_size = o.deletion_percentage * edge_list.size();
      size_t insertion_set_size = o.insert_percentage * edge_list.size();
      if (o.insert_percentage > 0.9999999999) {
        insertion_set_size = edge_list.size();
      }


      cout << "Creating " << insertion_set_size << " updates and " << deletion_set_size << " deletions." << endl;

      if (o.input_format == TEMPORAL_EDGELIST_TEXT) {
        sort(edge_list.begin(), edge_list.end(), greater<E>());
        cout << "Running as a temporal file." << endl;
      } else {
        shuffle(edge_list.begin(), edge_list.end(), std::mt19937(std::random_device()()));
        cout << "Running as non temporal file" << endl;
      }

      write_insertion_set<E>(edge_list.end() - insertion_set_size, edge_list.end());

      write_deletion_set<E>(edge_list.begin(), edge_list.begin() + deletion_set_size);

      if (edge_list.size() - insertion_set_size != 0) {
        if (!o.densify) {
          cout << "Cannot write base datasets without densifying first." << endl;
          exit(Options::BAD_CONF);
        } else {
          SortedCSRDataSource csr = convert_to_sorted_csr<E>(edge_list.begin(), edge_list.end() - insertion_set_size,
                                                             vertex_count, o.make_undirected);
          write_base_dataset(csr);

          write_degree_information(csr);
        }
      }
      cout << "End" << endl << endl;
    }

    template<typename E>
    vector<E> parse_text_file(Options o, size_t &vertex_count) {
      cout << "Parsing file " << o.input_path << "as edge type " << typeid(E).name() << " with densify " << o.densify
           << endl;

      vector<E> out;

      char seperator = detect_seperator(o.input_path);

      ifstream in{o.input_path};

      // Map for densifying
      unordered_map<string, size_t> translation;
      unordered_set<size_t> vertex_set;
      size_t next_vertex_id = 0;

      string line;
      while (getline(in, line)) {
        if (is_comment(line)) {
          continue;
        }

        E e = E::parse_edge(line, seperator, 2, o.densify, translation, next_vertex_id,
                            vertex_set);
        out.push_back(e);
      }

      if (o.densify) {
        vertex_count = next_vertex_id;
      } else {
        vertex_count = vertex_set.size();
      }

      in.close();
      return out;
    };


    template<typename E>
    void write_edle_list(const string &path, typename vector<E>::iterator begin, typename vector<E>::iterator end) {
      ofstream f{path, ofstream::out | ofstream::binary | ofstream::trunc};

      size_t count = end - begin;
      f.write((char *) &count, sizeof(count));

      auto pos = begin;
      auto i = 0;
      while (pos < end) {
        i++;
        pos->write_edge_to_binary_file(f);
        pos++;
      }

      f.close();
    };

    template<typename E>
    void write_insertion_set(typename vector<E>::iterator begin, typename vector<E>::iterator end) {
      cout << "Writing insertions to " << o.output_path + o.insertion_file_name << endl;
      write_edle_list<E>(o.output_path + o.insertion_file_name, begin, end);
    };

    template<typename E>
    void write_deletion_set(typename vector<E>::iterator begin, typename vector<E>::iterator end) {
      cout << "Writing deletions to " << o.output_path + o.deletion_file_name << endl;
      write_edle_list<E>(o.output_path + o.deletion_file_name, begin, end);
    };

    /**
     *
     * @param begin
     * @param end
     * @param filter Do not include these edges in the csr.
     * @param vertex_count
     * @return
     */
    template<typename E>
    SortedCSRDataSource convert_to_sorted_csr(typename vector<E>::iterator begin,
                                              typename vector<E>::iterator end,
                                              size_t vertex_count,
                                              bool make_undirected_base) {

      vector<E> undirected;
      if (make_undirected_base) {
        undirected = make_undirected<E>(begin, end);
        begin = undirected.begin();
        end = undirected.end();
      }

      // TODO support for weigths missing
      cout << "Creating CSR." << endl;
      sort(begin, end, [](E a, E b) { return a.src == b.src ? a.dst < b.dst : a.src < b.src; });

      SortedCSRDataSource out;
      out.adjacency_index.resize(vertex_count + 1);

      out.adjacency_lists.reserve(end - begin);

      vertex_id_t current_src = 0;

      while (current_src < begin->src) {
        out.adjacency_index[current_src] = 0;
        current_src++;
      }
      out.adjacency_index[current_src] = 0;

      auto pos = begin;
      while (pos < end) {
        if (current_src != pos->src) {
          while (current_src < pos->src) {
            out.adjacency_index[current_src + 1] = (pos - begin);
            current_src++;
          }
        }
        out.adjacency_lists.push_back(pos->dst);
        pos++;
      }

      while (current_src < vertex_count) {
        out.adjacency_index[current_src + 1] = (pos - begin);
        current_src++;
      }

      return out;
    }

    void write_base_dataset(SortedCSRDataSource csr);

    /**
     * For an edge list returns the edge list with all edges in opposite direction as well. This graph can
     * have many duplicate edges which need to be filtered later.
     *
     * @param edges
     * @return
     */
    template<typename E>
    vector<E> make_undirected(typename vector<E>::iterator begin,
                              typename vector<E>::iterator end) {
      cout << "Creating undirected dataset" << endl;
      vector<E> undirected;
      undirected.reserve((end - begin) * 2);

      for (; begin < end; begin++) {
        auto e = *begin;
        undirected.push_back(e);
        undirected.push_back(e.opposite());
      }

      return undirected;
    }

    /**
     * Removes self edges and duplicate edges.
     *
     * @return
     */
    template<typename E>
    vector<E> clean_data(vector<E> &edges, bool make_directed) {
      cout << "Cleaning data" << endl;
      vector<E> clean;
      clean.reserve(edges.size());

      unordered_set<E, EdgeHash<E>, EdgeEqual<E>> dedup;

      for (auto e : edges) {
        if (make_directed && e.dst < e.src) {
          swap(e.src, e.dst);
        }
        if (e.src != e.dst && dedup.find(e) == dedup.end()) {
          clean.push_back(e);
          dedup.insert(e);
        }
      }

      return clean;
    };

    void write_degree_information(SortedCSRDataSource &graph);
};


#endif //LIVE_GRAPH_TWO_DATASETCONVERTER_H
