//
// Created by per on 24.06.21.
//

#ifndef LIVE_GRAPH_TWO_VERSIONEDBLOCKEDPROPERTYEDGEITERATOR_H
#define LIVE_GRAPH_TWO_VERSIONEDBLOCKEDPROPERTYEDGEITERATOR_H

#include "VersionedBlockedEdgeIterator.h"

#define SORTLEDTON_ITERATE_WITH_PROPERTIES_NAMED(tx, src, edge_name, properties_name, end_label_name, on_edge) { \
  __label__ end_label_name;                                                                              \
  VersionedBlockedPropertyEdgeIterator _iter = tx.neighbourhood_with_properties_blocked_p(src); \
  while (_iter.has_next_block()) {             \
    auto [_versioned, _bs, _be, _ws, _we] = _iter.next_block_with_properties();                   \
    if (_versioned) {                          \
       while (_iter.has_next_edge()) {         \
         [[maybe_unused]] auto [edge_name, properties_name] = _iter.next_with_properties();                \
         on_edge\
       }                                           \
    } else {                                                                                                     \
      auto _p = _ws;                                                                                              \
      for (auto _i = _bs; _i < _be; _i++) {     \
        auto edge_name = *_i;                                                                                    \
        auto properties_name = *_p;                                                                               \
        _p++;                                                                                                         \
        on_edge\
      }                                           \
    }\
  }                                            \
  [[maybe_unused]] end_label_name: ; \
}

#define SORTLEDTON_WITH_PROPERTIES_ITERATE(tx, src, on_edge) SORTLEDTON_ITERATE_WITH_PROPERTIES_NAMED(tx, src, e, p, end_iteration, on_edge)


class VersionedBlockedPropertyEdgeIterator : public VersionedBlockedEdgeIterator {
public:
    VersionedBlockedPropertyEdgeIterator(VersioningBlockedSkipListAdjacencyList* ds, vertex_id_t v, dst_t* block,
                                         size_t size, bool versioned, version_t version, size_t property_size,
                                         weight_t* property_column, weight_t* properties_end  );

    VersionedBlockedPropertyEdgeIterator(VersioningBlockedSkipListAdjacencyList* ds, vertex_id_t v,
                                         VSkipListHeader* block, size_t block_size, bool versioned, version_t version,
                                         size_t property_size);

    /**
     * Moves the iterator to the next block.
     * has_next_edge() and next() can be used to iterate over the block.
     *
     * If the block is unversioned it can be iterated in a loop as contiguous memory from start to end to avoid function calls.
     *
     * Weights pointers are incorrect if the block is versioned.
     *
     * @return <is_versioned, start, end, weigths_start, weights_end>
     */
    tuple<bool, dst_t*, dst_t*, weight_t*, weight_t*> next_block_with_properties();

    /**
     *
     * @return the next edge in the current block.
     */
    pair<dst_t, weight_t> next_with_properties();


private:
    const size_t property_size;
    const size_t block_size;

    bool first_block = true;

    VSkipListHeader* n_block = nullptr;

    weight_t* property_column = nullptr;
    weight_t* properties_end = nullptr;
};


#endif //LIVE_GRAPH_TWO_VERSIONEDBLOCKEDPROPERTYEDGEITERATOR_H
