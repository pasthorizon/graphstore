//
// Created by per on 23.12.20.
//

#ifndef LIVE_GRAPH_TWO_VERSIONEDTOPOLOGYINTERFACE_H
#define LIVE_GRAPH_TWO_VERSIONEDTOPOLOGYINTERFACE_H

#include <vector>
#include "data_types.h"
#include "internal-driver/data-src/SortedCSRDataSource.h"

// TODO move to Topology interface.
// TODO move property interface to the topology interface
class NoProperties : exception {

};

class VersionedBlockedEdgeIterator;
class VersionedBlockedPropertyEdgeIterator;

using namespace std;

class VersionedTopologyInterface {
public:
    virtual ~VersionedTopologyInterface();

    virtual vertex_id_t logical_id(vertex_id_t id);
    virtual vertex_id_t physical_id(vertex_id_t id);

    virtual size_t vertex_count_version(version_t version) = 0;
    virtual size_t max_physical_vertex() = 0;
    virtual size_t edge_count_version(version_t version) = 0;

    virtual bool has_vertex_version(vertex_id_t v, version_t version);
    virtual bool has_vertex_version_p(vertex_id_t v, version_t version) = 0;

    /**
     * Will also acquire the vertex lock for the vertex in question, indepdent on if it is added or not.
     *
     * @param v
     * @param version
     * @return true if the vertex has been inserted by this call, false otherwise
     */
    virtual bool insert_vertex_version(vertex_id_t v, version_t version) = 0;
    virtual bool delete_vertex_version(vertex_id_t v, version_t version) = 0;

    virtual bool has_edge_version(edge_t edge, version_t version);
    virtual bool has_edge_version_p(edge_t edge, version_t version, bool debug = false) = 0;

    virtual bool get_weight_version(edge_t edge, version_t version, char* out);
    virtual bool get_weight_version_p(edge_t edge, version_t version, char* out) =0;

    virtual size_t neighbourhood_size_version(vertex_id_t src, version_t version);
    virtual size_t neighbourhood_size_version_p(vertex_id_t src, version_t version) = 0;

    virtual VersionedBlockedEdgeIterator neighbourhood_version_blocked_p(vertex_id_t src, version_t version) = 0;
    virtual VersionedBlockedPropertyEdgeIterator neighbourhood_version_blocked_with_properties_p(vertex_id_t src, version_t version) = 0;
    virtual void* raw_neighbourhood_version(vertex_id_t src, version_t version) = 0;
    virtual void intersect_neighbourhood_version(vertex_id_t a, vertex_id_t b, vector<dst_t>& out, version_t version) = 0;
    virtual void intersect_neighbourhood_version_p(vertex_id_t a, vertex_id_t b, vector<dst_t>& out, version_t version) = 0;

    virtual bool insert_edge_version(edge_t edge, version_t version) = 0;
    virtual bool insert_edge_version(edge_t edge, version_t version, char* properties, size_t properties_size) = 0;
    virtual bool delete_edge_version(edge_t edge, version_t version) = 0;

    virtual size_t get_property_size() = 0;

    /**
     *
     * @param vertex_lock
     * @return true if the lock has been acquired, false if the logical vertex does not exists and no lock has been aquired.
     */
    virtual bool aquire_vertex_lock(vertex_id_t vertex_lock) = 0;
    virtual void release_vertex_lock(vertex_id_t v) = 0;
    virtual void aquire_vertex_lock_p(vertex_id_t vertex_lock) = 0;
    virtual void release_vertex_lock_p(vertex_id_t v) = 0;
    virtual void aquire_vertex_lock_shared_p(vertex_id_t vertex_lock) = 0;
    virtual void release_vertex_lock_shared_p(vertex_id_t v) = 0;

    virtual void report_storage_size() = 0;

    virtual void bulkload(const SortedCSRDataSource& src) = 0;

    virtual void gc_all() = 0;
    virtual void gc_vertex(vertex_id_t v) = 0;

    virtual void rollback_vertex_insert(vertex_id_t v) = 0;
};


#endif //LIVE_GRAPH_TWO_VERSIONEDTOPOLOGYINTERFACE_H
