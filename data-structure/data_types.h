//
// Created by per on 31.08.20.
//

#ifndef LIVE_GRAPH_TWO_DATA_TYPES_H
#define LIVE_GRAPH_TWO_DATA_TYPES_H

#include <cstdint>
#include <ctime>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <unordered_set>
#include <algorithm>
#include <fstream>
#include <limits>

using namespace std;

  #define EXTRA_POINTERS 2
  #define MAX_EPOCHS 2
//#ifdef BITS64
// Used vertex identifier and destination data structure for all data structrues.
  typedef uint64_t vertex_id_t;
  typedef vertex_id_t dst_t;
  typedef double weight_t;

  // Version used to indicate that this is the first version of any version chain. This does not need to be
  // the original first version from system start but could be a later version after GC.
  #define FIRST_VERSION 0L
  #define NO_TRANSACTION numeric_limits<version_t>::max()
  // The first bit of a dst_t type is set if the edge is versioned.
  #define VERSION_MASK (1L << 63)

// version timestamp if the second bit is set there are further versions, if the third bit is set this version is a deletion.
// it is important that the first bit is never set
// TODO change this around, a first bit set indicates a version. while an unset first bit indicates that is not a version.
  typedef uint64_t version_t;
  #define MORE_VERSION_MASK (1L << 62)
  #define DELETION_MASK (1L << 61)
//#endif
#ifdef BITS32
// Used vertex identifier and destination data structure for all data structrues.
  typedef uint32_t vertex_id_t;
  typedef vertex_id_t dst_t;


  // Version used to indicate that this is the first version of any version chain. This does not need to be
  // the original first version from system start but could be a later version after GC.
  #define FIRST_VERSION 0
  // The first bit of a dst_t type is set if the edge is versioned.
  #define VERSION_MASK (1 << 31)

  // version timestamp if the second bit is set there are further versions, if the third bit is set this version is a deletion.
// it is important that the first bit is never set
// TODO change this around, a first bit set indicates a version. while an unset first bit indicates that is not a version.
  typedef uint32_t version_t;
  #define MORE_VERSION_MASK (1 << 30)
  #define DELETION_MASK (1 << 29)
#endif

#define make_versioned(e) (e | VERSION_MASK)
#define make_unversioned(e) (e & ~VERSION_MASK)
#define is_versioned(e) (e & VERSION_MASK)
#define is_deletion(e) (e & DELETION_MASK)
#define END_OF_PROPERTY '\0'
//bool is_versioned(dst_t e);
//
//dst_t make_versioned(dst_t e);
//
//dst_t make_unversioned(dst_t e);

bool more_versions_existing(version_t v);

// bool is_deletion(version_t v);

version_t timestamp(version_t v);

struct edge_t {
    vertex_id_t src;
    dst_t dst;

    edge_t() : src(0), dst(0) {};
    edge_t(vertex_id_t src, dst_t dst) : src(src), dst(dst) {};

    bool operator==(const edge_t other) const {
      return src == other.src && dst == other.dst;
    }

    static edge_t parse_edge(const string &line, const char seperator, size_t _,
                               bool densify, unordered_map<string, size_t> &densifyer, size_t &next_vertex_id,
                               unordered_set<size_t> &vertex_set) {
      if (densify) {
        return parse_edge_densifying(line, seperator, densifyer, next_vertex_id);
      } else {
        auto first_seperator_index = line.find(seperator);
        auto second_seperator_index = line.find(seperator, first_seperator_index + 1);

        string src_string = line.substr(0, first_seperator_index);
        string dst_string = line.substr(first_seperator_index + 1, second_seperator_index - first_seperator_index - 1);

        vertex_id_t src = stoi(src_string);
        dst_t dst = stoi(dst_string);

        vertex_set.insert(src);
        vertex_set.insert(dst);
        return edge_t{src, dst};
      }
    }

    static edge_t parse_edge_densifying(const string &line, const char seperator, unordered_map<string, size_t> &densifyer, size_t &next_vertex_id) {
      auto first_seperator_index = line.find(seperator);
      auto second_seperator_index = line.find(seperator, first_seperator_index + 1);

      string src_string = line.substr(0, first_seperator_index);
      string dst_string = line.substr(first_seperator_index + 1, second_seperator_index - first_seperator_index - 1);
      vertex_id_t src;
      dst_t dst;

      auto t = densifyer.find(src_string);
      if (t == densifyer.end()) {
        densifyer.insert(make_pair(src_string, next_vertex_id));
        src = next_vertex_id;
        next_vertex_id++;
      } else {
        src = t->second;
      }

      t = densifyer.find(dst_string);
      if (t == densifyer.end()) {
        densifyer.insert(make_pair(dst_string, next_vertex_id));
        dst = next_vertex_id;
        next_vertex_id++;
      } else {
        dst = t->second;
      }

      return edge_t{src, dst};
    }

    void write_edge_to_binary_file(ofstream& f) {
      f.write((char*) &src, sizeof(src));
      f.write((char*) &dst, sizeof(dst));
    }

    edge_t opposite() {
      return {dst, src};
    }

    bool operator > (const edge_t& other) const {
      return src == other.src ? (dst > other.dst) : (src > other.src);
    }
};

struct weighted_edge_t {
    vertex_id_t src;
    dst_t dst;
    weight_t weight;

    weighted_edge_t() : src(0), dst(0), weight(0.0) {};
    weighted_edge_t(vertex_id_t src, dst_t dst, weight_t weight) : src(src), dst(dst), weight(weight) {};
    weighted_edge_t(edge_t e) : weighted_edge_t(e.src, e.dst, 0.0) {};

    bool operator==(const edge_t other) const {
      return src == other.src && dst == other.dst;
    }

    static weighted_edge_t parse_edge(const string &line, const char seperator, size_t weight_position,
                               bool densify, unordered_map<string, size_t> &densifyer, size_t &next_vertex_id,
                               unordered_set<size_t> &vertex_set) {
      edge_t edge = edge_t::parse_edge(line, seperator, weight_position, densify, densifyer, next_vertex_id, vertex_set);

      weighted_edge_t t_edge (edge);

      if (weight_position != numeric_limits<size_t>::max()) {
        size_t weight_index = -1;
        for (auto i = 0u; i < weight_position; i++) {
          weight_index = line.find(seperator, weight_index + 1);
        }
        size_t afterTempIndex = line.find(seperator, weight_index + 1);
        t_edge.weight = stod(line.substr(weight_index + 1, afterTempIndex - weight_index - 1));
      }

      return t_edge;
    }

    void write_edge_to_binary_file(ofstream& f) {
      edge_t(src, dst).write_edge_to_binary_file(f);
      f.write((char*) &weight, sizeof(weight));
    }

    weighted_edge_t opposite() {
      return {dst, src, weight};
    }

    bool operator > (const weighted_edge_t& other) const {
      return src == other.src ? (dst > other.dst) : (src > other.src);
    }

};


struct temporal_edge_t {
    vertex_id_t src;
    dst_t dst;
    time_t creation_timestamp;

    temporal_edge_t(edge_t e) : src(e.src), dst(e.dst), creation_timestamp(0) {};
    temporal_edge_t(vertex_id_t src, dst_t dst, time_t time) : src(src), dst(dst), creation_timestamp(time) {};

    static temporal_edge_t parse_edge(const string &line, const char seperator, size_t temporal_value_position,
                                        bool densify, unordered_map<string, size_t> &densifyer, size_t &next_vertex_id,
                                        unordered_set<size_t> &vertex_set) {
      edge_t edge = edge_t::parse_edge(line, seperator, temporal_value_position, densify, densifyer, next_vertex_id, vertex_set);

      temporal_edge_t t_edge (edge);

      if (temporal_value_position != numeric_limits<size_t>::max()) {
        size_t tempIndex = -1;
        for (auto i = 0u; i < temporal_value_position; i++) {
          tempIndex = line.find(seperator, tempIndex + 1);
        }
        size_t afterTempIndex = line.find(seperator, tempIndex + 1);
        t_edge.creation_timestamp = stoi(line.substr(tempIndex + 1, afterTempIndex - tempIndex - 1));
      }

      return t_edge;
    }

    void write_edge_to_binary_file(ofstream& f) {
      edge_t(src, dst).write_edge_to_binary_file(f);
      f.write((char*) &creation_timestamp, sizeof(creation_timestamp));
    }

    temporal_edge_t opposite() {
      return {dst, src, creation_timestamp};
    }

    bool operator > (const temporal_edge_t& other) const {
      return creation_timestamp > other.creation_timestamp;
    }
};

template <typename E>
struct EdgeEqual {
public:
    bool operator()(const E& a, const E& b) const {
      return a.src == b.src && a.dst == b.dst;
    }
};

template <typename E>
struct EdgeHash {
public:

    size_t operator()(const E& e) const {
      return std::hash<vertex_id_t>()(e.src) + 31 * std::hash<dst_t>()(e.dst);
    }
};

#endif //LIVE_GRAPH_TWO_DATA_TYPES_H
