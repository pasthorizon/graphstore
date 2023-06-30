//
// Created by per on 03.12.20.
//

#ifndef LIVE_GRAPH_TWO_PAGERANK_H
#define LIVE_GRAPH_TWO_PAGERANK_H

#include <vector>

#include "internal-driver/Driver.h"
#include "data-structure/TopologyInterface.h"

class PageRank {
public:
    static vector<pair<vertex_id_t, double>> page_rank(Driver& driver, TopologyInterface& ds, int iterations, double damping_factor, bool use_raw_neighbourhood, bool use_gapbs);
    static vector<double> page_rank_bs(TopologyInterface& ds, int iterations, double damping_factor);
};


#endif //LIVE_GRAPH_TWO_PAGERANK_H
