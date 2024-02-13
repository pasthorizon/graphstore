//
// Created by per on 23.02.21.
//

#ifndef LIVE_GRAPH_TWO_VERTEXINDEX_H
#define LIVE_GRAPH_TWO_VERTEXINDEX_H

#include <cassert>
#include <cstdint>
#include <atomic>
#include <optional>
#include <vector>
#include <iostream>
#include <math.h>

#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_vector.h>
#include <tbb/concurrent_queue.h>
#include <mutex>

#include "data-structure/data_types.h"
#include "SizeVersionChainEntry.h"
#include "AdjacencySetTypes.h"
#include "utils/pointerTagging.h"
#include "third-party/RWSpinLock.h"

#include "AccessPointers.h"

using namespace std;

// The mask indicating if a size entry in the index is versioned.
#define SIZE_VERSION_OFFSET 63
#define SIZE_VERSION_MASK (1L << SIZE_VERSION_OFFSET)
// The 2nd bit of the adjacency set pointer in the index is used to indicate the VAdjacencySetType.
// Set means the edge set is of type VSINGLE_BLOCK, unset means it is of type VSKIP_LIST
#define EDGE_SET_TYPE_MASK (1L << 62)
#define LOCK_MASK (1L << 61)
// This mask is set on vertex index entries for unused vertices.
#define VERTEX_NOT_USED_MASK (1L << 60)

#define INITIAL_VECTOR_SIZE 131072

struct VertexVersionChainEntry;

//struct VertexEntry {
//
//public:
//    uint64_t get_block_capacity();
//
//    VAdjacencySetType get_adjacency_set_type();
//    VSkipListHeader* get_skip_list_header();
//    dst_t* get_block();
//
//    uint64_t get_size(version_t version);
//    VertexVersionChainEntry* get_vertex_versions();
//    SizeVersionChainEntry* get_size_versions();
//private:
//    void* adjacency_set;  // Holds a pointer to the adjacency set or the version chain entry for this vector
//    uint64_t size;        // Holds the size of the adjacency set of this vector or the capacity of the vector represesnting the adjacency set or the version chain entry for the size.
//
//    bool is_size_versioned();
//};
//
//struct VertexVersionChainEntry {
//    VertexVersionChainEntry* next;
//    version_t version;
//    VertexEntry e;
//};

struct VertexEntry {
    AllInlineAccessPointersWithSize adjacency_set;
    RWSpinLock lock {};

    uint64_t wait_time_aggregate = 0;
    uint64_t num_invoke = 0;

    atomic<uint64_t> wait_time_aggregate_shared{0ul};
    atomic<uint64_t> num_invoke_shared{0ul};

    VertexEntry() {
        
    }
    VertexEntry(const VertexEntry& other) {
      
    }
};

typedef tbb::concurrent_hash_map<vertex_id_t, vertex_id_t> l_t_p_table;

class VertexIndex {
public:
    VertexIndex();

    VertexIndex(const VertexIndex &) = delete;

    VertexIndex &operator=(const VertexIndex &) = delete;

    optional<vertex_id_t> physical_id(vertex_id_t v);

    vertex_id_t logical_id(vertex_id_t v);

    bool has_vertex(vertex_id_t v);

    bool insert_vertex(vertex_id_t id, version_t version);

    bool remove_vertex(vertex_id_t id, version_t version);

    void aquire_vertex_lock_p(vertex_id_t v);

    void release_vertex_lock_p(vertex_id_t v);

    bool aquire_vertex_lock(const vertex_id_t v);

    void release_vertex_lock(const vertex_id_t v);

    void aquire_vertex_lock_shared_p(const vertex_id_t v);

    void release_vertex_lock_shared_p(const vertex_id_t v);

    VertexEntry const &operator[](size_t index) const;

    VertexEntry &operator[](size_t index);

    size_t get_vertex_count(version_t version);

    size_t get_high_water_mark();

    void rollback_vertex_insert(vertex_id_t v);

    

    inline version_t get_latest_version(vertex_id_t v, version_t version){
      return index[v].adjacency_set.get_latest_version();
    }

    inline void create_new_version(vertex_id_t v, version_t version, version_t min_active_version){
      void *latest_pointer = index[v].adjacency_set.get_latest_pointer();
      // uint64_t start = __rdtsc();
      index[v].adjacency_set.add_new_pointer(latest_pointer, version, min_active_version);
      // uint64_t end = __rdtsc();
      // index[v].wait_time_aggregate += end - start;
      return;
    }

    inline void* neighbourhood_version(vertex_id_t v, version_t version) {
      return (void *) ((uint64_t)index[v].adjacency_set.get_pointer(version) & (~EDGE_SET_TYPE_MASK));
    };

    inline VAdjacencySetType get_adjacency_set_type(vertex_id_t v, version_t version) {
      if ((uint64_t)index[v].adjacency_set.get_pointer(version) & EDGE_SET_TYPE_MASK) {
        return VSINGLE_BLOCK;
      } else {
        return VSKIP_LIST;
      }
    };

    inline void store_single_block(vertex_id_t v, dst_t* block, uint64_t capacity, uint64_t size, version_t version, version_t min_active=0){

      assert(index[v].adjacency_set.get_latest_version()==version);
      index[v].adjacency_set.pointers[0] = (void*)((uint64_t) block | EDGE_SET_TYPE_MASK);
      set_block_size(v, capacity, size, version);
    }

    //DONT NEED THIS ONE
    // inline bool size_is_versioned(vertex_id_t v) {
    //   return index[v].size & SIZE_VERSION_MASK;
    // };


    inline tuple<uint64_t, version_t> get_version_size(vertex_id_t v, version_t version){
      return index[v].adjacency_set.get_size(version);
    }

    inline tuple<uint64_t, uint64_t, uint64_t> get_single_block_size(vertex_id_t v, version_t version) {
      assert(get_adjacency_set_type(v, version) == VSINGLE_BLOCK);
      auto [size, curr_version] = get_version_size(v, version);
      return {size & 0x00000000FFFFFFFF, (size & 0xFFFFFFFF00000000) >> 32, curr_version};
    };

    inline void set_block_size(vertex_id_t v, uint64_t capacity, uint64_t size, version_t version) {
      assert(capacity < pow(2, 16));
      assert(size < pow(2, 16));
      assert(get_adjacency_set_type(v, version) == VSINGLE_BLOCK);
      // std::cout<<size<<std::endl;
      size = (size << 32 | capacity);
      index[v].adjacency_set.sizes[0] = size;
      // std::cout<<"finally size "<<index[v].adjacency_set.sizes[0]<<std::endl;
    }

    //DO NOT NEED THIS ONE
    // inline void* raw_neighbourhood_size_entry(vertex_id_t v) {
    //   return (void*) index[v].size;
    // };


    bool has_vertex_version_p(vertex_id_t v, version_t version) {
      return !((uint64_t)index[v].adjacency_set.pointers[0] & VERTEX_NOT_USED_MASK);
    };


private:
    atomic_uint high_water_mark{0u};  // The next physical vertex id, not yet in use.
    atomic_uint vertex_count{0u};

    mutex growing_vector_mutex;

    tbb::concurrent_vector<VertexEntry> index{INITIAL_VECTOR_SIZE, VertexEntry()};

    l_t_p_table logical_to_physical{INITIAL_VECTOR_SIZE};
    tbb::concurrent_vector<vertex_id_t> physical_to_logical;  // Cannot use initializer (size, default value) here because it will create a vector of size 2

    tbb::concurrent_queue<vertex_id_t> free_list {};


    template<typename T>
    void grow_vector_if_smaller(tbb::concurrent_vector<T> &v, size_t s, T init_value) {
      if (v.capacity() <= s) {  // Only synchronize with other threads if potentially necessary
        scoped_lock<mutex> l(growing_vector_mutex);
        if (v.capacity() <= s) {
          v.grow_to_at_least(v.capacity() * 2, init_value);
        }

      }
    }

};


#endif //LIVE_GRAPH_TWO_VERTEXINDEX_H
