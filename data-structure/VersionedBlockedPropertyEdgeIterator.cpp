//
// Created by per on 24.06.21.
//

#include "VersionedBlockedPropertyEdgeIterator.h"

#include "EdgeBlock.h"

VersionedBlockedPropertyEdgeIterator::VersionedBlockedPropertyEdgeIterator(VersioningBlockedSkipListAdjacencyList *ds,
                                                                           vertex_id_t v, dst_t *block, size_t size,
                                                                           version_t version,
                                                                           size_t property_size,
                                                                           weight_t* property_column)
        : VersionedBlockedEdgeIterator(ds, v, block, size, version), property_size(property_size),
          block_size(0), n_block(nullptr), property_column(property_column), version(version) {
}

VersionedBlockedPropertyEdgeIterator::VersionedBlockedPropertyEdgeIterator(VersioningBlockedSkipListAdjacencyList *ds,
                                                                           vertex_id_t v, VSkipListHeader *block,
                                                                           size_t block_size,
                                                                           version_t version,
                                                                           size_t property_size)
        : VersionedBlockedEdgeIterator(ds, v, block, version), property_size(property_size),
          block_size(block_size), version(version)
           {
  auto eb = EdgeBlock::from_vskip_list_header(block, block_size, property_size);
  property_column = (weight_t*) eb.properties_start();
  properties_end = (weight_t*) eb.properties_end() + 1;

  n_block = (VSkipListHeader*)block->next_levels[0]->get_pointer(version);
}

tuple<dst_t *, dst_t *, weight_t *, weight_t *>
VersionedBlockedPropertyEdgeIterator::next_block_with_properties() {
  auto [e_b, e_e] = VersionedBlockedEdgeIterator::next_block();
  assert(property_column != nullptr);

  if (!first_block) {
    if (n_block != nullptr) {
      auto eb = EdgeBlock::from_vskip_list_header(n_block, block_size, property_size);
      property_column = (weight_t *) eb.properties_start();
      properties_end = (weight_t *) eb.properties_end();
      n_block = (VSkipListHeader*) n_block->next_levels[0]->get_pointer(version);
    } else {
      property_column = nullptr;
      properties_end = nullptr;
    }
  } else {
    first_block = false;
  }
  return make_tuple(e_b, e_e, property_column, properties_end);
}

pair<dst_t, weight_t> VersionedBlockedPropertyEdgeIterator::next_with_properties() {
  auto e = VersionedBlockedEdgeIterator::next();
  auto w = *property_column;
  property_column += 1;
  return make_pair(e, w);
}
