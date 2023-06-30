//
// Created by per on 21.07.20.
//

#ifndef GRAPH_CONVERTER_OPTIONS_H
#define GRAPH_CONVERTER_OPTIONS_H

#include <string>
#include <limits>

using namespace std;

enum InputType {
    EDGELIST_TEXT,
    TEMPORAL_EDGELIST_TEXT,
    WEIGHTED_EDGELIST_TEXT   // A file with one edge per line: <src dst weight>, weight should be parseable to a double. Examples for this format can be found in the Graphalytics datasets
};


class Options {
public:
    static const int BAD_CONF = 2;
    static const int BAD_FORMAT = 3;

    string base_file_name;
    string insertion_file_name;
    string deletion_file_name;

    InputType input_format = EDGELIST_TEXT;
    size_t temporal_value_position = numeric_limits<size_t>::max();

    bool make_undirected = false;

    float insert_percentage = 0.0;
    float deletion_percentage = 0.0;

    bool densify = false;

    string input_path = "";
    string output_path = "";

    void validate();

    Options() {
      base_file_name = "base.csr";
      insertion_file_name = "insertions.edgeList";
      deletion_file_name = "deletions.edgeList";
    }
};


Options parseOptions(int argc, char **argv);


#endif //GRAPH_CONVERTER_OPTIONS_H
