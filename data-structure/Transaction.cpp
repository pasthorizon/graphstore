//
// Created by per on 23.12.20.
//

#include "Transaction.h"

void Transaction::use_does_not_exists_semantics() {
  use_edge_does_not_exists_semantics();
  use_vertex_does_not_exists_semantics();
}
