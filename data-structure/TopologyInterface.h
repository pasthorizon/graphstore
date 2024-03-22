#ifndef LIVE_GRAPH_TWO_TOPLOGYINTERFACE_H
#define LIVE_GRAPH_TWO_TOPLOGYINTERFACE_H

#include "internal-driver/data-src/DataSource.h"
#include "internal-driver/data-src/SortedCSRDataSource.h"
#include "data_types.h"
#include "utils/NotImplemented.h"

class VersionedBlockedEdgeIterator;

class EdgeExistsException : exception {
public:
    const edge_t edge;
    explicit EdgeExistsException(edge_t e) : edge(e) {  };
};

class EdgeDoesNotExistsException : exception {
public:
    const edge_t edge;
    explicit EdgeDoesNotExistsException(edge_t e) : edge(e) {  };
};


class VertexExistsException : exception {
public:
    const vertex_id_t vertex;
    explicit VertexExistsException(vertex_id_t v) : vertex(v) {};
};

class VertexDoesNotExistsException : exception {
public:
    const vertex_id_t vertex;
    explicit VertexDoesNotExistsException(vertex_id_t v) : vertex(v) {};
};


class TopologyInterface {
public:
//    TopologyInterface();
    virtual ~TopologyInterface();

    virtual vertex_id_t logical_id(vertex_id_t id);
    virtual vertex_id_t physical_id(vertex_id_t id);

    virtual size_t vertex_count() = 0;
    virtual size_t max_physical_vertex();
    virtual size_t edge_count() = 0;

    virtual bool insert_vertex(vertex_id_t v) = 0;
    virtual bool delete_vertex(vertex_id_t v) = 0;

    virtual bool insert_edge(edge_t edge) = 0;
    virtual bool insert_edge(edge_t edge, char* properties, size_t property_size) { throw NotImplemented(); };
    virtual bool insert_safe(edge_t edge) = 0;
    virtual bool delete_edge(edge_t edge) = 0;

    virtual bool has_vertex(vertex_id_t v);
    virtual bool has_vertex_p(vertex_id_t v) = 0;

    virtual size_t neighbourhood_size(vertex_id_t src);
    virtual size_t neighbourhood_size_p(vertex_id_t src) = 0;

    virtual VersionedBlockedEdgeIterator neighbourhood_blocked_p(vertex_id_t src);
    virtual void* raw_neighbourhood(vertex_id_t src) = 0;
    virtual void intersect_neighbourhood(vertex_id_t a, vertex_id_t b, vector<dst_t>& out);
    virtual void intersect_neighbourhood_p(vertex_id_t a, vertex_id_t b, vector<dst_t>& out) = 0;

    virtual bool has_edge(edge_t edge);
    virtual bool has_edge_p(edge_t edge) = 0;

    virtual void bulkload(const SortedCSRDataSource& src) = 0;

    virtual void report_storage_size() = 0;

    virtual void update_wait_time_shared_p(vertex_id_t src, uint64_t wait_time, int num_invoke) = 0;
};


#endif //LIVE_GRAPH_TWO_TOPLOGYINTERFACE_H
