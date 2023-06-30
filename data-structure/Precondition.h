//
// Created by per on 23.12.20.
//

#ifndef LIVE_GRAPH_TWO_PRECONDITION_H
#define LIVE_GRAPH_TWO_PRECONDITION_H


#include "VersionedTopologyInterface.h"

class Precondition {
public:
    virtual ~Precondition();

    virtual void assert_it(VersionedTopologyInterface& ds, version_t version) = 0;

    /**
     * For preconditions requiring only a single vertex lock.
     * @return
     */
    virtual vertex_id_t requires_vertex_lock();

    /**
     * For preconditions requiring multiple vertex locks. Will only be called if
     * requires_vertex_lock returns numeric_limits<vertex_id_t>::max()
     * @return
     */
    virtual vector<vertex_id_t> requires_vertex_locks() = 0;
};


#endif //LIVE_GRAPH_TWO_PRECONDITION_H
