//
// Created by per on 23.12.20.
//

#ifndef LIVE_GRAPH_TWO_TRANSACTION_H
#define LIVE_GRAPH_TWO_TRANSACTION_H

#include "data_types.h"
#include "utils/NotImplemented.h"
#include "TopologyInterface.h"
#include "Precondition.h"

class VersionedPropertyEdgeIterator;
class VersionedBlockedEdgeIterator;
class VersionedBlockedPropertyEdgeIterator;

class Transaction : public TopologyInterface {
public :
    virtual void use_does_not_exists_semantics();
    virtual void use_vertex_does_not_exists_semantics() = 0;
    virtual void use_edge_does_not_exists_semantics() = 0;

    bool insert_safe(edge_t e) override { throw NotImplemented(); }
    virtual bool insert_or_update_edge(edge_t edge, char* properties, size_t property_size) = 0;

    // TODO lower to topology interface
    virtual bool get_weight(edge_t edge, char* out) = 0;
    virtual bool get_weight_p(edge_t edge, char* out) = 0;

    // TODO lower to topology interface
    virtual VersionedBlockedPropertyEdgeIterator neighbourhood_with_properties_blocked_p(vertex_id_t src) = 0;

    virtual version_t get_version() const = 0;
    virtual version_t get_commit_version() const = 0;
};


#endif //LIVE_GRAPH_TWO_TRANSACTION_H
