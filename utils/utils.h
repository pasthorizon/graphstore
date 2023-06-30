//
// Created by per on 31.08.20.
//

#ifndef LIVE_GRAPH_TWO_UTILS_H
#define LIVE_GRAPH_TWO_UTILS_H

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "data-structure/data_types.h"

using namespace std;

bool file_exists (const string& name);
bool endsWith(const string& fullString, const string& ending);

string string_join(const string& join, const vector<string>& list);

template<typename n>
n sum(const vector<n>& v) {
  n s = 0;
  for (const auto& l : v) {
    s += l;
  }
  return s;
}


template<typename k, typename v>
unordered_map<v, k> reverse_map(unordered_map<k, v> mapping) {
  unordered_map<v, k> out;
  for (const auto& kv : mapping) {
    out.insert({kv.second, kv.first});
  }
  return out;
}

string get_filename(string path);

string get_home_dir();

void intersect_edge_block(dst_t* start_a, dst_t* end_a, dst_t* start_b, dst_t* end_b, vector<dst_t>& out);

template<typename K, typename V>
unordered_set<V> get_values_from_multimap(unordered_multimap<K, V> map, K key) {
  unordered_set<V> r;
  r.reserve(map.count(key));

  auto range = map.equal_range(key);

  auto i = range.first;
  while(i != range.second) {
    r.insert(i->second);
    i++;
  }
  return r;
}

uint round_up_power_of_two(uint v);

/**
 * Source: https://stackoverflow.com/questions/216823/whats-the-best-way-to-trim-stdstring
 */

// trim from start (in place)
static inline void ltrim(std::string &s) {
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
      return !std::isspace(ch);
  }));
}

// trim from end (in place)
static inline void rtrim(std::string &s) {
  s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) {
      return !std::isspace(ch);
  }).base(), s.end());
}

// trim from both ends (in place)
static inline void trim(std::string &s) {
  ltrim(s);
  rtrim(s);
}

#endif //LIVE_GRAPH_TWO_UTILS_H
