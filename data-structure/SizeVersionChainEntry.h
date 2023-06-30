//
// Created by per on 13.01.21.
//

#ifndef LIVE_GRAPH_TWO_SIZEVERSIONCHAINENTRY_H
#define LIVE_GRAPH_TWO_SIZEVERSIONCHAINENTRY_H

#include <data-structure/data_types.h>

class SizeVersionChainEntry {
public:
    version_t version;
    uint32_t current_size;

    SizeVersionChainEntry(version_t version, uint32_t current_size);
};


#endif //LIVE_GRAPH_TWO_SIZEVERSIONCHAINENTRY_H
