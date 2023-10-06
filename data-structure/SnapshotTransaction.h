//
// Created by per on 23.12.20.
//

#ifndef LIVE_GRAPH_TWO_SNAPSHOTTRANSACTION_H
#define LIVE_GRAPH_TWO_SNAPSHOTTRANSACTION_H

#include <memory>
#include <vector>
#include <set>
#include <mutex>

#include "Transaction.h"
#include "IllegalOperation.h"
#include "VertexExistsPrecondition.h"

class RollbackAction {
public:
    enum ROLLBACK_ACTION {
        INSERT_VERTEX
    };

    ROLLBACK_ACTION type;
    vertex_id_t  vertex;
    edge_t edge;  // Unused currently

    static RollbackAction generate_rollback_insert_vertex(vertex_id_t v) {
      return RollbackAction(INSERT_VERTEX, v, edge_t(0, 0));
    }

private:
    RollbackAction(ROLLBACK_ACTION type, vertex_id_t v, edge_t e) : type(type), vertex(v), edge(e) {};
};

class TransactionManager;

class SnapshotTransaction : public Transaction {
public:
    SnapshotTransaction(TransactionManager* tm, bool write_only, VersionedTopologyInterface* ds, bool analytics = false);
    ~SnapshotTransaction();

    void use_vertex_does_not_exists_semantics() override;
    void use_edge_does_not_exists_semantics() override;

    void register_precondition(Precondition* c);

    vertex_id_t physical_id(vertex_id_t v) override;
    vertex_id_t logical_id(vertex_id_t v) override;

    bool execute();

    size_t vertex_count() override;
    size_t max_physical_vertex() override;

    bool has_vertex(vertex_id_t v) override;
    bool has_vertex_p(vertex_id_t v) override;
    bool insert_vertex(vertex_id_t v) override;
    bool delete_vertex(vertex_id_t v) override;

    size_t edge_count() override;
    bool insert_edge(edge_t edge) override;
    bool insert_edge(edge_t edge, char* properties, size_t property_size) override;
    bool insert_or_update_edge(edge_t edge, char* properties, size_t property_size) override;
    bool delete_edge(edge_t edge) override;

    size_t neighbourhood_size_p(vertex_id_t src) override;

    VersionedBlockedEdgeIterator neighbourhood_blocked_p(vertex_id_t src) override;
    VersionedBlockedPropertyEdgeIterator neighbourhood_with_properties_blocked_p(vertex_id_t src) override;

    /**
     * Cannot be used. Use raw_ds instead.
     */
    void* raw_neighbourhood(vertex_id_t src) override { throw NotImplemented(); };

    /**
     *  Be aware that you need to handle locking and versioning yourself.
     *
     * Passing any other version than the one of this transaction is undefined behaviour.
     *
     * Using the pointer after calling execute or TransactionManager.transactionCompleted is undefined behaviour.
     */
    VersionedTopologyInterface* raw_ds();

    void intersect_neighbourhood_p(vertex_id_t a, vertex_id_t b, vector<dst_t>& out) override;

    bool has_edge_p(edge_t edge) override;
    bool get_weight(edge_t edge, char* out) override;
    bool get_weight_p(edge_t edge, char* out) override;

    void bulkload(const SortedCSRDataSource& src) override;

    void report_storage_size() override;

    version_t get_version() const override;
    version_t  get_commit_version() const override;
    void set_read_timestamp(version_t timestamp);
    version_t read_version = NO_TRANSACTION;
    void clear(bool write_only);
protected:
    
    version_t commit_version = NO_TRANSACTION;
    VersionedTopologyInterface* ds;

private:
    // TODO check how much performanc it cost to make this class thread safe by a mutex on each writing function.
    // if this is to expensive reintroduce a thread safe readonly transaction and note about thread safety in the documentation.

    void aquire_locks_and_insert_vertices();
    void release_locks();
    void rollback();
    void assert_preconditions();
    void assert_std_preconditions();

    static std::set<pair<int,int>> alledges;
    static mutex lock;

    TransactionManager* tm;
    bool write_only = false;

    /**
     * Used for communication between aquire_ and release_locks. This is the last lock aquire_locks locked.
     * release_lock will only call unlock for locks up till then.
     */
    vertex_id_t  last_lock_aquired = 0;

    bool vertex_does_not_exists_semantic_activated = false;
    bool edge_does_not_exists_semantic_activated = false;

    /**
     * Used to store information on the number of vertices in the data structure at the time of transaction start.
     *
     * This is a unclean workaround as I did not implement vertex versioning.
     */
    size_t max_physical_vertex_id = 0;
    size_t number_of_vertices = 0;

    vector<Precondition*> preconditions {};
    vector<vertex_id_t> locks_to_aquire {};
    vector<vertex_id_t> vertices_to_delete {};
    vector<vertex_id_t> vertices_to_insert {};
    vector<vertex_id_t> vertices_to_insert_if_not_exists {};
    vector<edge_t> edges_to_delete {};
    vector<tuple<edge_t, char*, size_t>> edges_to_update_or_insert {};
    vector<tuple<edge_t, char*, size_t>> edges_to_insert {};

    vector<RollbackAction> rollbacks {};

    void rewrite_inserted_vertex_timestamps();
};


#endif //LIVE_GRAPH_TWO_SNAPSHOTTRANSACTION_H
