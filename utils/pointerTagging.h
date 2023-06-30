//
// Created by per on 12.03.21.
//

#ifndef LIVE_GRAPH_TWO_POINTERTAGGING_H
#define LIVE_GRAPH_TWO_POINTERTAGGING_H


inline uint64_t get_pointer(uint64_t pointer_number) {
  return 0x0000FFFFFFFFFFFFul & pointer_number;
}

inline uint64_t is_set(uint64_t pointer_number, uint tag_bit) {
  return pointer_number & (1ul << tag_bit);
}

inline uint64_t set(uint64_t pointer_number, uint tag_bit) {
  return pointer_number | (1ul << tag_bit);
}

inline uint64_t unset(uint64_t pointer_number, uint tag_bit) {
  return pointer_number &  ~(0ul << tag_bit);
}

#endif //LIVE_GRAPH_TWO_POINTERTAGGING_H
