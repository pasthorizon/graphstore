//
// Created by per on 13.04.21.
//

#include "VersioningBlockedSkipListAdjacencyList.h"
#include "VersionedBlockedEdgeIterator.h"
#include "EdgeVersionRecord.h"
#include <iostream>

#include <utils/NotImplemented.h>

VersionedBlockedEdgeIterator::VersionedBlockedEdgeIterator(VersioningBlockedSkipListAdjacencyList* ds, vertex_id_t v,dst_t *block, size_t _capacity, size_t size, size_t _property_size, version_t version)
        : ds(ds), src(v), block(block), current_block_end(block + _capacity), version(version), data(block), eb(block,_capacity, size, _property_size) {
          property_size = _property_size;
          capacity = _capacity;
          isasingleblock = true;
          open();
}

VersionedBlockedEdgeIterator::VersionedBlockedEdgeIterator(VersioningBlockedSkipListAdjacencyList* ds, vertex_id_t v, VSkipListHeader *block, size_t _capacity, size_t _property_size, version_t version)
    : ds(ds), src(v), version(version), eb(block != nullptr? block->data:nullptr, _capacity, block != nullptr? block->size:0, _property_size) {
      isasingleblock=false;
      capacity = _capacity;
      property_size = _property_size;
      if (block == nullptr) {
        n_block = nullptr;
        this->block = nullptr;
        current_block_end = nullptr;
        current_block_is_versioned = false;
        data = nullptr;
      } else {
        n_block = ds->get_latest_next_pointer(block, 0,version);
        this->block = eb.get_single_block_pointer();
        current_block_end = eb.get_single_block_pointer() + eb.get_max_edge_index();
        data = eb.get_single_block_pointer();
      }
      open();
}

bool VersionedBlockedEdgeIterator::has_next_block() {
  if (first_block && block != nullptr) {
    first_block = false;
    // std::cout<<"yup:"<<*block<<" "<<src<<"\n";
    return true;
  } else if (n_block != nullptr) {
    eb = EdgeBlock::from_vskip_list_header(n_block, capacity, property_size);
    block = n_block->data;
    current_block_end = block + eb.get_max_edge_index();
    
    n_block = ds->get_latest_next_pointer(n_block,0,version);
    data = block;
    return true;
  }
  return false;
}

tuple<dst_t *, dst_t*> VersionedBlockedEdgeIterator::next_block() {
  return make_tuple(block, current_block_end);
}

VersionedBlockedEdgeIterator::~VersionedBlockedEdgeIterator() {
  close();
}

void VersionedBlockedEdgeIterator::open() {
  opened = true;
//  ds->aquire_vertex_lock_shared_p(src);
}

void VersionedBlockedEdgeIterator::close() {
  ds->release_vertex_lock_shared_p(src);
  opened = false;
}

bool VersionedBlockedEdgeIterator::is_open() {
  return opened;
}

bool VersionedBlockedEdgeIterator::has_next_edge() {
  return move_to_next_edge_in_current_block();
}

//TO DO: FIX THIS
bool VersionedBlockedEdgeIterator::move_to_next_edge_in_current_block() {
  while (data < current_block_end) {
    current_edge = *data;
    data+=1;
  }
  return false;
}


dst_t VersionedBlockedEdgeIterator::next() {
  return current_edge;
}


