//
// Created by per on 31.08.20.
//

#ifndef LIVE_GRAPH_TWO_SORTEDCSRDATASOURCE_H
#define LIVE_GRAPH_TWO_SORTEDCSRDATASOURCE_H

#include <unordered_set>
#include <cstddef>
#include <string>
#include <vector>
#include "DataSource.h"
#include "data-structure/data_types.h"

using namespace std;

class SortedCSRDataSource : DataSource {
public:
    void read_from_binary_file(const string& path);
    size_t vertex_count() const { return adjacency_index.size() - 1; };
    size_t edge_count() const { return adjacency_lists.size(); };
    size_t neighbourhood_size(vertex_id_t v) const { return adjacency_index[v+1] - adjacency_index[v]; };
    size_t get_min_neighbour(vertex_id_t v) const { return adjacency_lists[adjacency_index[v]]; };
    size_t get_max_neighbour(vertex_id_t v) const { return adjacency_lists[adjacency_index[v+1] - 1]; };

    vector<size_t> adjacency_index;
    vector<dst_t> adjacency_lists;

    struct FileHeader {
        size_t vertex_count;
        size_t edge_count;
    };
    struct FileBody {
        size_t* offsets; // Array of offsets of length vertex_count + 1
        dst_t* adjacency_lists; // Array of all adjacency information of length edge_count.
    };

    unordered_set<dst_t> get_neighbour_set(vertex_id_t v);
};


#endif //LIVE_GRAPH_TWO_SORTEDCSRDATASOURCE_H
