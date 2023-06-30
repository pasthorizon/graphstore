//
// Created by per on 30.03.21.
//

#ifndef LIVE_GRAPH_TWO_EXPERIMENTS_H
#define LIVE_GRAPH_TWO_EXPERIMENTS_H

#include <unordered_map>

enum Experiments {
    INSERT_TRANSACTIONS,  // Inserts edges one-by-one using one transaction per edge.
    DELETE,
    BFS,
    PR,
    STORAGE,
    GC,
    GAPBS_BFS,
    GAPBS_PR,
    SSSP,
    WCC,
    LCC,
    CDLP
};


#endif //LIVE_GRAPH_TWO_EXPERIMENTS_H
