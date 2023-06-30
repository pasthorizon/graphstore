//
// Created by per on 31.08.20.
//

#include "utils.h"
#include <sys/stat.h>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <cctype>

bool file_exists(const string &name) {
  struct stat buffer;
  return (stat(name.c_str(), &buffer) == 0);
}

/**
 * https://stackoverflow.com/questions/874134/find-out-if-string-ends-with-another-string-in-c
 * @param fullString
 * @param ending
 * @return
 */
bool endsWith(string const &fullString, string const &ending) {
  if (fullString.length() >= ending.length()) {
    return (0 == fullString.compare(fullString.length() - ending.length(), ending.length(), ending));
  } else {
    return false;
  }
}

string get_filename(string path) {
  char sep = '/';

  auto pos = path.find_last_of(sep);
  if (pos == string::npos) {
    return path;
  } else {
    return path.substr(pos + 1, path.size());
  }
}

string get_home_dir() {
  return string(getenv("HOME"));
}

string string_join(const string &join, const vector<string> &list) {
  stringstream ss;

  for (auto i = 0u; i < list.size(); i++) {
    if (i != 0) {
      ss << join;
    }
    ss << list[i];
  }
  return ss.str();
}

void intersect_edge_block(dst_t *start_a, dst_t *end_a, dst_t *start_b, dst_t *end_b, vector<dst_t> &out) {
  out.clear();
  set_intersection(start_a, end_a, start_b, end_b, back_inserter(out));
}

uint round_up_power_of_two(uint v) {
  v--;
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  v++;
  return v;
}
