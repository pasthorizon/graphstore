//
// Created by per on 23.12.20.
//

#include "Precondition.h"

Precondition::~Precondition() {

}

vertex_id_t Precondition::requires_vertex_lock() {
  return numeric_limits<vertex_id_t>::max();
}
