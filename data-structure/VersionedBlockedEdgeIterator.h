//
// Created by per on 13.04.21.
//

#ifndef LIVE_GRAPH_TWO_VERSIONEDBLOCKEDEDGEITERATOR_H
#define LIVE_GRAPH_TWO_VERSIONEDBLOCKEDEDGEITERATOR_H

#include <optional>
#include<iostream>

#include <data-structure/data_types.h>
#include "AdjacencySetTypes.h"
#include "TransactionManager.h"
#include "VersioningBlockedSkipListAdjacencyList.h"
// TODO define early stop

#define SORTLEDTON_ITERATE_NAMED(tx, src, edge_name, end_label_name, on_edge) { \
  __label__ end_label_name;  \
  VersionedBlockedEdgeIterator _iter = tx.neighbourhood_blocked_p(src); \
  while (_iter.has_next_block()) {             \
    auto [_bs, _be] = _iter.next_block(); bool flag = false; \                                    
    for(auto _i = _bs; _i < _be; _i++) {     \
      auto edge_name = *_i;           \
        on_edge                         \
      } \                                           
  }      \
  [[maybe_unused]] end_label_name: ; \
}

#define SORTLEDTON_ITERATE(tx, src, on_edge) SORTLEDTON_ITERATE_NAMED(tx, src, e, end_iteration, on_edge)



// class VersioningBlockedSkipListAdjacencyList;

class VersionedBlockedEdgeIterator {
public:
    VersionedBlockedEdgeIterator(VersioningBlockedSkipListAdjacencyList* ds, vertex_id_t v, dst_t* block, size_t size, version_t version);
    VersionedBlockedEdgeIterator(VersioningBlockedSkipListAdjacencyList* ds, vertex_id_t v, VSkipListHeader* block, version_t version);

    VersionedBlockedEdgeIterator(VersionedBlockedEdgeIterator& other) = delete;
    VersionedBlockedEdgeIterator& operator=(const VersionedBlockedEdgeIterator&) = delete;

    ~VersionedBlockedEdgeIterator();

    bool has_next_block();

    /**
     * Moves the iterator to the next block.
     * has_next_edge() and next() can be used to iterate over the block.
     *
     * If the block is unversioned it can be iterated in a loop as contiguous memory from start to end to avoid function calls.
     *
     * @return <is_versioned, start, end>
     */
    tuple<dst_t*, dst_t*> next_block();

    /**
     *
     * @return true if there are still edges in the current block, false otherwise
     */
    bool has_next_edge();

    /**
     *
     * @return the next edge in the current block.
     */
    dst_t next();

    void open();
    void close();
    bool is_open();
bool isasingleblock;
VersioningBlockedSkipListAdjacencyList* ds = nullptr;
version_t  version = NO_TRANSACTION;
private:
    
    vertex_id_t src = 0;
  
    VSkipListHeader* n_block = nullptr;

    dst_t* block = nullptr;
    dst_t* current_block_end = 0;
    bool current_block_is_versioned = false;
    

    dst_t* data = nullptr;
    dst_t current_edge = 0;

    bool first_block = true;
    bool opened = false;

    bool move_to_next_edge_in_current_block();
};


#endif //LIVE_GRAPH_TWO_VERSIONEDBLOCKEDEDGEITERATOR_H


