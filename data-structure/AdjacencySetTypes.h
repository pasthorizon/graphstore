//
// Created by per on 24.03.21.
//

#ifndef LIVE_GRAPH_TWO_ADJACENCYSETTYPES_H
#define LIVE_GRAPH_TWO_ADJACENCYSETTYPES_H

#define SKIP_LIST_LEVELS 6

#include "AccessPointers.h"
/**
 * The types of adjacency sets used.
 */
enum VAdjacencySetType {
    VSKIP_LIST,    // A blocked skip list defined in VSkipListHeader
    VSINGLE_BLOCK  // An array of edges prepended by the number of edges and versions in their.
};

struct VSkipListHeader {
    VSkipListHeader *before;  // TODO remove
    dst_t *data;
    uint16_t height;
    uint16_t size;  // Number of destinations stored in this block.
    dst_t max;
    AccessPointers *next_levels[SKIP_LIST_LEVELS];
    version_t version;
    
    char* property_start(size_t block_size, size_t property_size) {
      return (char*) data + block_size * sizeof(dst_t) + block_size * sizeof(weight_t);
    }
};

#endif //LIVE_GRAPH_TWO_ADJACENCYSETTYPES_H