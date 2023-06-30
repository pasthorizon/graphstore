//
// Created by per on 24.03.21.
//

#ifndef LIVE_GRAPH_TWO_ADJACENCYSETTYPES_H
#define LIVE_GRAPH_TWO_ADJACENCYSETTYPES_H

#define SKIP_LIST_LEVELS 6

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
    uint16_t size;  // Number of destinations stored in this block.
    uint16_t properties;
    dst_t max;
    VSkipListHeader *next_levels[SKIP_LIST_LEVELS];  // a fixed number of pointers for all levels.

    char* property_start(size_t block_size, size_t property_size) {
      return (char*) data + block_size * sizeof(dst_t) + block_size * property_size - properties * property_size;
    }
};

#endif //LIVE_GRAPH_TWO_ADJACENCYSETTYPES_H