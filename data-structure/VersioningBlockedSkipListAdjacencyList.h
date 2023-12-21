//
// Created by per on 23.12.20.
//

#ifndef LIVE_GRAPH_TWO_VERSIONINGBLOCKEDSKIPLISTADJACENCYLIST_H
#define LIVE_GRAPH_TWO_VERSIONINGBLOCKEDSKIPLISTADJACENCYLIST_H


#include <mutex>
#include <random>
#include <atomic>
#include <forward_list>
#include <queue>
#include <utils/NotImplemented.h>
#include <data-structure/TransactionManager.h>
#include <data-structure/SizeVersionChainEntry.h>
#include "VersionedTopologyInterface.h"
#include "memory_allocation/BlockProvider.h"

#include "VertexIndex.h"
#include "EdgeBlock.h"

#include "FreeList.h"

enum AdjacencySetType {
    SKIP_LIST,
    SINGLE_BLOCK
};

class VersionedBlockedEdgeIterator;

class VersioningBlockedSkipListAdjacencyList : public VersionedTopologyInterface {

public:
    VersioningBlockedSkipListAdjacencyList(size_t block_size, size_t property_size, TransactionManager& tm);
    ~VersioningBlockedSkipListAdjacencyList() override;

    vertex_id_t physical_id(vertex_id_t v) override;
    vertex_id_t logical_id(vertex_id_t v) override;

    size_t vertex_count_version(version_t version) override;
    size_t max_physical_vertex() override;
    size_t edge_count_version(version_t version) override;

    // TODO vertex versioning not yet supported
    bool has_vertex_version(vertex_id_t v, version_t version) override;
    bool has_vertex_version_p(vertex_id_t v, version_t version) override;

    // TODO versioning not yet supported
    bool insert_vertex_version(vertex_id_t v, version_t version) override;
    bool delete_vertex_version(vertex_id_t v, version_t version) override { throw NotImplemented(); };

    size_t neighbourhood_size_version_p(vertex_id_t src, version_t version) override;

    void* raw_neighbourhood_version(vertex_id_t src, version_t version) override;
    VAdjacencySetType get_set_type(vertex_id_t v, version_t version);
    void* raw_neighbourhood_size_entry(vertex_id_t v);
    VersionedBlockedEdgeIterator neighbourhood_version_blocked_p(vertex_id_t src, version_t version) override;
    VersionedBlockedPropertyEdgeIterator neighbourhood_version_blocked_with_properties_p(vertex_id_t src, version_t version) override;

    void intersect_neighbourhood_version(vertex_id_t a, vertex_id_t b, vector<dst_t>& out, version_t version) override { throw NotImplemented(); };
    void intersect_neighbourhood_version_p(vertex_id_t a, vertex_id_t b, vector<dst_t>& out, version_t version) override;

    bool has_edge_version_p(edge_t edge, version_t version, bool debug = false) override;
    bool get_weight_version_p(edge_t edge, version_t version, char* out) override;

    bool insert_edge_version(edge_t edge, version_t version) override;
    bool insert_edge_version(edge_t edge, version_t version, char* properties, size_t properties_size) override;
    bool delete_edge_version(edge_t edge, version_t version) override;

    size_t get_property_size() override;

    bool aquire_vertex_lock(vertex_id_t v) override;
    void release_vertex_lock(vertex_id_t v) override;
    void aquire_vertex_lock_p(vertex_id_t vertex_lock) override;
    void release_vertex_lock_p(vertex_id_t v) override;
    void aquire_vertex_lock_shared_p(vertex_id_t vertex_lock) override;
    void release_vertex_lock_shared_p(vertex_id_t v) override;

    void createEpoch(version_t version) override;

    void report_storage_size() override;
    
    VSkipListHeader* get_latest_next_pointer(VSkipListHeader *pHeader, uint16_t level, version_t version);


    /**
     * Bulkload data from a CSR. Does not write any versions. Only to be used with an empty data structure.
     */
    void bulkload(const SortedCSRDataSource &src);

    size_t get_block_size();

    void gc_all() override;
    static void gc_thread(version_t version, int id);
    void gc_vertex(vertex_id_t v) override;

    thread_local static int gced_edges;
    thread_local static int gc_merges;
    thread_local static int gc_to_single_block;

    thread_local static FreeList local_free_list;

    void rollback_vertex_insert(vertex_id_t v) override;
    VertexIndex adjacency_index;

protected:
    bool gc_block(vertex_id_t v);
    bool gc_skip_list(vertex_id_t v);

    /**
     * Removes all versions older than min_version from to_clean.
     *
     * Moves versions to before if possible, otherwise moves versions to after.
     *
     * This does collapse skip lists into a single single blocked skip list, it does not collapse it to a smaller adjacency
     * list of type VSingleBlock. This is the responsibility of the caller.
     *
     * @param to_clean the block to remove old versions from, this is an out parameter, it is either the same as for input or a nullptr if to_clean has been removed from the list
     * @param before the block before to_clean, can be a nullptr, is stable after this function
     * @param after the block after to_clean, can be a nullptr, is stable after this function
     * @param min_version minimal version to keep
     * @param blocks all blocks from the skip list that point to from that is one per level of from. This function
     * guarantues not too touch any of these elements if they do not point to from.
     * @param leave_space when pulling elements from the block before or merging blocks, keep leave_space free places to allow for inserts or deletions which run afterwards.
     * @return true if there are still versioned edges in edges to to_clean. Although, they might have been moved to before or after.
     */
    bool gc_skip_list_block(VSkipListHeader **to_clean, VSkipListHeader *before,
            VSkipListHeader *after, version_t min_version, VSkipListHeader* blocks[SKIP_LIST_LEVELS],
            int leave_space);
private:
    TransactionManager& tm;
    
    std::queue<version_t> versions;
    
    size_t block_size;
    size_t property_size;
    const float bulk_load_fill_rate = 1.0;

    // Skiplist constant, likelyhood for being x level high is p^x. 0.25 is a typical value from prior work.
    const float p = 0.5;
    static thread_local mt19937 level_generator;

//    BlockProvider pool;

    void* write_to_blocks(const dst_t* start, const dst_t* end);

    size_t memory_block_size();

    size_t get_height();




    void add_new_pointer(VSkipListHeader *pHeader, uint16_t level, VSkipListHeader *pointer, version_t version);
    VSkipListHeader* copy_skip_list_block(dst_t src, VSkipListHeader *block, VSkipListHeader *blocks_per_level[SKIP_LIST_LEVELS], version_t version, int type = 0);

    VSkipListHeader* find_block(VSkipListHeader *pHeader, version_t version, dst_t element, VSkipListHeader* blocks[SKIP_LIST_LEVELS]);
    VSkipListHeader* find_block1(VSkipListHeader *pHeader, version_t version, dst_t element);

    size_t skip_list_header_size() const;
    dst_t* get_data_pointer(VSkipListHeader* header) const;

    dst_t* find_upper_bound(dst_t* start, uint16_t size, dst_t value);

    EdgeBlock new_single_edge_block(size_t min_capicity_in_edges);
    VSkipListHeader* new_skip_list_block();

    void insert_empty(edge_t edge, version_t version, char* properties);
    void insert_single_block(edge_t edge, version_t version, char* properties);
    void insert_skip_list(edge_t edge, version_t version, char* properties);

    // bool size_is_versioned(vertex_id_t v);

    void update_adjacency_size(vertex_id_t v, bool deletion, version_t version);
    forward_list<SizeVersionChainEntry>* construct_version_chain_from_block(vertex_id_t v, version_t version);

    forward_list<SizeVersionChainEntry>::iterator get_version_from_chain(forward_list<SizeVersionChainEntry> &chain, version_t version);
    /**
     * Garbage collects unnecessary versions from a adjacency size version chain. These are all version which are
     * smaller than collect_after.
     *
     * @param start the start of the version chain.
     * @param collect_after timestamp of the minimal version to keep
     * @return a list of now unused version chain entries which can be reused
     */
    forward_list<SizeVersionChainEntry> gc_adjacency_size(forward_list<SizeVersionChainEntry>& chain, version_t collect_after);

    /**
     * Removes block from the skip list by merging it into its predecessor or successor.
     *
     * @param block Block to remove
     * @param head head of the skiplist the blocks belongs to.
     */
    void merge_skip_list_blocks(VSkipListHeader* block, VSkipListHeader* head, vertex_id_t src, version_t version);

    /**
     * Converts a SkipList adjacency list with only one block back into a single block.
     *
     * Does nothing if the SkipList is still half full.
     *
     * frees SkipList block if it is converted.
     *
     * @param v vertex id for which to convert the adjacency set.
     * @param contains_versions if the block still contains any versions.
     */
    void skip_list_to_single_block(vertex_id_t v, version_t versions);

    void assert_adjacency_list_consistency(vertex_id_t v, version_t min_version, version_t current_version);
    size_t assert_edge_block_consistency(EdgeBlock eb, vertex_id_t src, version_t version);

    dst_t get_min_from_skip_list_header(VSkipListHeader* header);

    /**
     * Assumes that the adjacency set is unversioned.
     * @param v
     */
    void free_adjacency_set(vertex_id_t v);

    size_t get_max_vertex();

    size_t get_single_block_memory_size(size_t capacity);

    void* get_block(size_t size);
    static void free_block(void* block, size_t size);

    size_t low_skiplist_block_bound();

    /**
     * Balances the block with either the block before or after.
     *
     * If the number of edges in this block and its neighbours are less than the threshold it merges the block.
     */
    void balance_block(VSkipListHeader* block, VSkipListHeader* head, vertex_id_t src, version_t version);

    bool delete_from_single_block(edge_t edge, version_t version);

    bool delete_skip_list(edge_t edge, version_t version);
};


#endif //LIVE_GRAPH_TWO_VERSIONINGBLOCKEDSKIPLISTADJACENCYLIST_H
