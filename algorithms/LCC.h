//
// Created by per on 26.03.21.
//

#ifndef LIVE_GRAPH_TWO_LCC_H
#define LIVE_GRAPH_TWO_LCC_H


#include "data-structure/data_types.h"
#include "data-structure/TopologyInterface.h"
#include "internal-driver/Driver.h"

class LCC {
public:
    static vector<pair<vertex_id_t, double>> lcc(Driver& driver, TopologyInterface& ds);

    static vector<double> lcc_merge_sort(TopologyInterface& ds);
};


#endif //LIVE_GRAPH_TWO_LCC_H
