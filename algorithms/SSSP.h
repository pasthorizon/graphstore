//
// Created by per on 26.03.21.
//

#ifndef LIVE_GRAPH_TWO_SSSP_H
#define LIVE_GRAPH_TWO_SSSP_H

#include "internal-driver/Driver.h"
#include <optional>

class SSSP {

public:
    static vector<pair<vertex_id_t, weight_t>> sssp(TopologyInterface& ds, bool use_raw_neighbourhoud, uint64_t source_vertex_id, double delta);
    static vector<weight_t> gabbs_sssp(TopologyInterface& ds, uint64_t physical_source, double delta);
};


#endif //LIVE_GRAPH_TWO_SSSP_H
