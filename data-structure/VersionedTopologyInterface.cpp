//
// Created by per on 23.12.20.
//

#include "VersionedTopologyInterface.h"
#include<iostream>

VersionedTopologyInterface::~VersionedTopologyInterface() {

}

bool VersionedTopologyInterface::has_vertex_version(vertex_id_t v, version_t version) {
  return has_vertex_version_p(physical_id(v), version);
}

vertex_id_t VersionedTopologyInterface::logical_id(vertex_id_t id) {
  return id;
}

vertex_id_t VersionedTopologyInterface::physical_id(vertex_id_t id) {
  return id;
}

bool VersionedTopologyInterface::has_edge_version(edge_t edge, version_t version) {
  return has_edge_version_p(edge_t(physical_id(edge.src), physical_id(edge.dst)), version);
}

size_t VersionedTopologyInterface::neighbourhood_size_version(vertex_id_t src, version_t version) {
  return neighbourhood_size_version_p(physical_id(src), version);
}

bool VersionedTopologyInterface::get_weight_version(edge_t edge, version_t version, char* out) {
  return get_weight_version_p(edge_t(physical_id(edge.src), physical_id(edge.dst)), version, out);
}
