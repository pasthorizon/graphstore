//
// Created by per on 13.04.21.
//

#include "VersioningBlockedSkipListAdjacencyList.h"
#include "VersionedBlockedEdgeIterator.h"
#include "EdgeVersionRecord.h"

#include <utils/NotImplemented.h>

VersionedBlockedEdgeIterator::VersionedBlockedEdgeIterator(VersioningBlockedSkipListAdjacencyList* ds, vertex_id_t v,dst_t *block, size_t size, bool versioned, version_t version)
        : ds(ds), src(v), block(block), current_block_end(block + size), current_block_is_versioned(versioned), version(version), data(block) {
  open();
}

VersionedBlockedEdgeIterator::VersionedBlockedEdgeIterator(VersioningBlockedSkipListAdjacencyList* ds, vertex_id_t v, VSkipListHeader *block, bool versioned, version_t version)
    : ds(ds), src(v), version(version) {
  if (block == nullptr) {
    n_block = nullptr;
    this->block = nullptr;
    current_block_end = nullptr;
    current_block_is_versioned = false;
    data = nullptr;
  } else {
    n_block = block->next_levels[0];
    this->block = block->data;
    current_block_end = block->data + block->size;
    current_block_is_versioned = versioned;  // TODO could be handled on a per block basis if Skiplistheaders had information on this.
    data = block->data;
  }
  open();
}

bool VersionedBlockedEdgeIterator::has_next_block() {
  if (first_block && block != nullptr) {
    first_block = false;
    return true;
  } else if (n_block != nullptr) {
    block = n_block->data;
    current_block_end = block + n_block->size;
    n_block = n_block->next_levels[0];

    data = block;
    return true;
  }
  return false;
}

tuple<bool, dst_t *, dst_t*> VersionedBlockedEdgeIterator::next_block() {
  return make_tuple(current_block_is_versioned, block, current_block_end);
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

bool VersionedBlockedEdgeIterator::move_to_next_edge_in_current_block() {
  while (data < current_block_end) {
    if (is_versioned(*data)) {
      const EdgeVersionRecord vr {make_unversioned(*data), data + 1, nullptr, false, 0};
      bool exists = vr.exists_in_version(version);
      if (exists) {
        current_edge = make_unversioned(*data);
        data += 2;
        return true;
      } else {
        data += 2;
      }
    } else {
      current_edge = *data;
      data += 1;
      return true;
    }
  }
  return false;
}


dst_t VersionedBlockedEdgeIterator::next() {
  return current_edge;
}


