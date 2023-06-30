//
// Created by per on 26.03.21.
//

#ifndef LIVE_GRAPH_TWO_WCC_H
#define LIVE_GRAPH_TWO_WCC_H

#include "internal-driver/Driver.h"

class WCC {
public:
    static vector<pair<vertex_id_t, vertex_id_t>> wcc(Driver& driver, TopologyInterface& ds, bool run_on_raw_neighbourhoud);
    static vector<vertex_id_t> gapbs_wcc(TopologyInterface& ds);
};


#endif //LIVE_GRAPH_TWO_WCC_H
