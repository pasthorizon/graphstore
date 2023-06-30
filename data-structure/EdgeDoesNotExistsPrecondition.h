//
// Created by per on 29.01.21.
//

#ifndef LIVE_GRAPH_TWO_EDGEDOESNOTEXISTSPRECONDITION_H
#define LIVE_GRAPH_TWO_EDGEDOESNOTEXISTSPRECONDITION_H

#include "Precondition.h"


class EdgeDoesNotExistsPrecondition : public Precondition {
public:
    explicit EdgeDoesNotExistsPrecondition(edge_t e);
    ~EdgeDoesNotExistsPrecondition() override;

    void assert_it(VersionedTopologyInterface& ds, version_t version) override;
    vector<vertex_id_t> requires_vertex_locks() override;
    vertex_id_t requires_vertex_lock() override;

private:
    edge_t e;
};


#endif //LIVE_GRAPH_TWO_EDGEDOESNOTEXISTSPRECONDITION_H
