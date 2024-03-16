//
// Created by per on 23.02.21.
//

#include <cassert>
#include <optional>
#include <iostream>

#include "VertexIndex.h"
#include "TopologyInterface.h"

vertex_id_t VertexIndex::logical_id(vertex_id_t v) {
  assert(v < high_water_mark);
  return physical_to_logical[v];
}

optional<vertex_id_t> VertexIndex::physical_id(vertex_id_t v) {
  l_t_p_table::const_accessor a;
  logical_to_physical.find(a, v);
  return a.empty() ? nullopt : make_optional(a->second);
}

VertexEntry const &VertexIndex::operator[](size_t v) const {
  return index[v];
}

VertexEntry &VertexIndex::operator[](size_t v) {
  return index[v];
}

bool VertexIndex::insert_vertex(vertex_id_t id, version_t version) {
  vertex_id_t p_id;
  l_t_p_table::accessor w;
  if (logical_to_physical.insert(w, id)) {
    if (!free_list.try_pop(p_id)) {
      p_id = high_water_mark.fetch_add(1);

      grow_vector_if_smaller(index, p_id, VertexEntry());  // TODO Should I do this earlier and assynchronous
      grow_vector_if_smaller(physical_to_logical, p_id, (0ul | VERTEX_NOT_USED_MASK));
    }

    w->second = p_id;
    aquire_vertex_lock_p(p_id);
    w.release();

    // Update index
    assert((uint64_t)index[p_id].adjacency_set.pointers[0] & VERTEX_NOT_USED_MASK);
    index[p_id].adjacency_set.pointers[0] = (void*) nullptr ;
    index[p_id].adjacency_set.sizes[0] = 0;

    // Update logical mapping
    assert(physical_to_logical[p_id] & VERTEX_NOT_USED_MASK);
    physical_to_logical[p_id] = id;

    // Update vertex count
    vertex_count.fetch_add(1);
    return true;
  } else {
    p_id = w->second;
    w.release();
    aquire_vertex_lock_p(p_id);
    return false;
  }
}

size_t VertexIndex::get_high_water_mark() {
  return high_water_mark.load();
}

size_t VertexIndex::get_vertex_count(version_t version) {
  return vertex_count.load();
}

void VertexIndex::aquire_vertex_lock_p(vertex_id_t v) {
  // TODO with the c++20 flag implementation we could do this: https://rigtorp.se/spinlock/
  uint64_t start = __rdtsc();
  index[v].lock.lock();
  uint64_t end = __rdtsc();

  index[v].wait_time_aggregate += end - start;
  index[v].wait_time_num_invoke ++;
}

void VertexIndex::aquire_vertex_lock_shared_p(const vertex_id_t v) {
  uint64_t start = __rdtsc();
  index[v].lock.lock_shared();
  uint64_t end = __rdtsc();
  index[v].wait_time_aggregate_shared += end-start;
  index[v].wait_time_shared_num_invoke ++;
  // cout<<"hello world"<<endl;
}

void VertexIndex::release_vertex_lock_shared_p(const vertex_id_t v) {
  index[v].lock.unlock_shared();
}


void VertexIndex::release_vertex_lock_p(vertex_id_t v) {
  index[v].lock.unlock();
}

bool VertexIndex::aquire_vertex_lock(const vertex_id_t v) {
  l_t_p_table::const_accessor a;
  if (logical_to_physical.find(a, v)) {
    auto p_id = a->second;
    a.release();
    aquire_vertex_lock_p(p_id);
    // TODO if I also allow vertex removal could this fail?
    return true;
  } else {
    return false;
  }
}

void VertexIndex::release_vertex_lock(vertex_id_t v) {
  {
    l_t_p_table::const_accessor a;
    if (logical_to_physical.find(a, v)) {
      release_vertex_lock_p(a->second);
    } else {
      throw VertexDoesNotExistsException(v);
    }
  }
}

void VertexIndex::rollback_vertex_insert(vertex_id_t v) {
  l_t_p_table::accessor a;
  if (!logical_to_physical.find(a, v)) {
    auto p_id = a->second;
    index[v].adjacency_set.pointers[0] = (void*)(0ul | VERTEX_NOT_USED_MASK);
    index[v].adjacency_set.sizes[0] = (0ul | VERTEX_NOT_USED_MASK);
    physical_to_logical[p_id] = 0l | VERTEX_NOT_USED_MASK;
    logical_to_physical.erase(a);
    free_list.push(p_id);
  } else {
    throw VertexDoesNotExistsException(v);
  }
}

bool VertexIndex::has_vertex(vertex_id_t v) {
  l_t_p_table::const_accessor a;
  return logical_to_physical.find(a, v);
}

VertexIndex::VertexIndex() {
  physical_to_logical = tbb::concurrent_vector<vertex_id_t>(INITIAL_VECTOR_SIZE, 0l | VERTEX_NOT_USED_MASK);
}
