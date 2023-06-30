//
// Created by per on 29.01.21.
//

#include "EdgeDoesNotExistsPrecondition.h"

#include <iostream>

#include "data-structure/TopologyInterface.h"

EdgeDoesNotExistsPrecondition::EdgeDoesNotExistsPrecondition(edge_t e) : e(e) {

}

void EdgeDoesNotExistsPrecondition::assert_it(VersionedTopologyInterface &ds, version_t version) {
  if (ds.has_edge_version(e, version)) {
    throw EdgeExistsException(e);
  }
}

vector<vertex_id_t> EdgeDoesNotExistsPrecondition::requires_vertex_locks() {
  return vector<vertex_id_t>(1, e.src);
}

vertex_id_t EdgeDoesNotExistsPrecondition::requires_vertex_lock() {
  return e.src;
}

EdgeDoesNotExistsPrecondition::~EdgeDoesNotExistsPrecondition() {

}
