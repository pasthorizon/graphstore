//
// Created by per on 30.03.21.
//

#ifndef LIVE_GRAPH_TWO_GRAPHALYTICSPROPERTIES_H
#define LIVE_GRAPH_TWO_GRAPHALYTICSPROPERTIES_H

#include <filesystem>
#include <vector>
#include <unordered_set>

#include "Experiments.h"

using namespace std;
using namespace filesystem;

/**
 * Represents and parses property files from the Graphalytic Benchmark
 */
class GraphalyticsProperties {
public:
    GraphalyticsProperties();
    GraphalyticsProperties(path property_file );

    bool is_empty();

    path base_dir {};
    string graph_name {""};
    int vertices = 0;
    int edges = 0;
    int cdlp_max_iterations = 0;
    int bfs_source_vertex = 0;
    double page_rank_damping_factor = 0;
    int page_rank_num_iterations = 0;
    int sssp_source_vertex = 0;
    unordered_set<Experiments> experiments {};

    bool supports_experiment(Experiments experiments);

    string insertion_set();
    string base_graph();

    string gold_standard(string e);

    bool is_graphalytic_experiment(Experiments e);

private:
    const static string PROPERTY_FILE_EXTENSION;
    const static unordered_map<string, Experiments> algorithm_to_experiment;

    bool empty = true;

    void parse(const path& property_file);
    bool is_comment(const string& line);
    vector<string> split(const string& s, const char& c);

    void parse_algorithms(const string& algorithm_list);
};


#endif //LIVE_GRAPH_TWO_GRAPHALYTICSPROPERTIES_H
