//
// Created by per on 24.06.21.
//

#include "VersionedBlockedPropertyEdgeIterator.h"

#include "EdgeBlock.h"

VersionedBlockedPropertyEdgeIterator::VersionedBlockedPropertyEdgeIterator(VersioningBlockedSkipListAdjacencyList *ds,
                                                                           vertex_id_t v, dst_t *block, size_t size,
                                                                           bool versioned, version_t version,
                                                                           size_t property_size,
                                                                           weight_t* property_column,
                                                                           weight_t* properties_end)
        : VersionedBlockedEdgeIterator(ds, v, block, size, versioned, version), property_size(property_size),
          block_size(0), n_block(nullptr), property_column(property_column), properties_end(properties_end) {
}

VersionedBlockedPropertyEdgeIterator::VersionedBlockedPropertyEdgeIterator(VersioningBlockedSkipListAdjacencyList *ds,
                                                                           vertex_id_t v, VSkipListHeader *block,
                                                                           size_t block_size,
                                                                           bool versioned, version_t version,
                                                                           size_t property_size)
        : VersionedBlockedEdgeIterator(ds, v, block, versioned, version), property_size(property_size),
          block_size(block_size),
          n_block(block->next_levels[0]) {
  auto eb = EdgeBlock::from_vskip_list_header(block, block_size, property_size);
  property_column = (weight_t*) eb.properties_start();
  properties_end = (weight_t*) eb.properties_end() + 1;
}

tuple<bool, dst_t *, dst_t *, weight_t *, weight_t *>
VersionedBlockedPropertyEdgeIterator::next_block_with_properties() {
  auto [versioned, e_b, e_e] = VersionedBlockedEdgeIterator::next_block();
  assert(property_column != nullptr);

  if (!first_block) {
    if (n_block != nullptr) {
      auto eb = EdgeBlock::from_vskip_list_header(n_block, block_size, property_size);
      property_column = (weight_t *) eb.properties_start();
      properties_end = (weight_t *) eb.properties_end();
      n_block = n_block->next_levels[0];
    } else {
      property_column = nullptr;
      properties_end = nullptr;
    }
  } else {
    first_block = false;
  }
  return make_tuple(versioned, e_b, e_e, property_column, properties_end);
}

pair<dst_t, weight_t> VersionedBlockedPropertyEdgeIterator::next_with_properties() {
  auto e = VersionedBlockedEdgeIterator::next();
  auto w = *property_column;
  property_column += 1;
  return make_pair(e, w);
}
