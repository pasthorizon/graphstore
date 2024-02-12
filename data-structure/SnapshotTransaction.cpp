//
// Created by per on 23.12.20.
//

#include <data-structure/VersioningBlockedSkipListAdjacencyList.h>

#include <iostream>
#include <fstream>
#include "SnapshotTransaction.h"
#include "VersionedBlockedEdgeIterator.h"
#include "VersionedBlockedPropertyEdgeIterator.h"
#include "TransactionManager.h"

typedef struct edgeversion {
  vertex_id_t src;
  vertex_id_t dst;
  version_t version;
  bool insert;
};
std::vector<edgeversion> alledges;
mutex SnapshotTransaction::lock;

SnapshotTransaction::SnapshotTransaction(TransactionManager* tm, bool write_only, VersionedTopologyInterface *ds, bool analytics)
        : tm(tm), write_only(write_only), ds(ds), m_analytics(false) {
  if (!write_only) {
    read_version =   tm->get_epoch();
    if(analytics){
      read_version--;
      m_analytics = true;
    }
    if (ds != nullptr) {
      max_physical_vertex_id = ds->max_physical_vertex();
      number_of_vertices = ds->vertex_count_version(read_version);
    }
  }
}

bool SnapshotTransaction::execute() {
  try {
    aquire_locks_and_insert_vertices();
    commit_version = tm->get_epoch();
    rewrite_inserted_vertex_timestamps();
    assert_preconditions();
    assert_std_preconditions();
    for (auto v: vertices_to_delete) {
      if (vertex_does_not_exists_semantic_activated && !ds->has_vertex_version(v, commit_version)) {
        continue;
      }
      ds->delete_vertex_version(v, commit_version);
    }
    for (auto e : edges_to_delete) {
      edge_t p_edge (ds->physical_id(e.src), ds->physical_id(e.dst));
      if (edge_does_not_exists_semantic_activated && !ds->has_edge_version_p(p_edge, commit_version)) {
        continue;
      }
      // if(p_edge.src == 59652|| p_edge.src ==  815752)
      // cout<<"deleting edge: "<<p_edge.src<<" "<<p_edge.dst<<endl;
      ds->delete_edge_version(p_edge, commit_version);

      // if(ds->has_edge_version_p(p_edge, commit_version)){
      //   cout<<"\nedge found after deletion\n";
      //   cout<<p_edge.src<<" "<<p_edge.dst<<endl;
      //   ds->has_edge_version_p(p_edge, commit_version, true);
      //   throw EdgeExistsException(e);
      // }
      // alledges.push_back({p_edge.src, p_edge.dst, commit_version, false});
    }
//    auto i = 0;
    for (auto [e, properties, properties_size] : edges_to_insert) {
      

      edge_t p_edge (ds->physical_id(e.src), ds->physical_id(e.dst));

//      try {
      if (edge_does_not_exists_semantic_activated && ds->has_edge_version_p(p_edge, commit_version)) {
        continue;
      }

      // 15087 609451
      // if(p_edge.src == 59652|| p_edge.src ==  815752)
      // cout<<"inserting edge "<<p_edge.src<<" "<<p_edge.dst<<endl;

      ds->insert_edge_version(p_edge, commit_version, properties, properties_size);
      // if(p_edge.src == 59652|| p_edge.src ==  815752)
      // cout<<"insertion done: "<<p_edge.src<<" "<<p_edge.dst<<endl;
      // if(!ds->has_edge_version_p(p_edge, commit_version)){
      //   cout<<"\nedge not found after insertion\n";
      //   cout<<p_edge.src<<" "<<p_edge.dst<<endl;
      //   ds->has_edge_version_p(p_edge, commit_version, true);

      //   ds->insert_edge_version(p_edge, commit_version,properties, properties_size, true);
      //   exit(0);
      //   // throw EdgeDoesNotExistsException(e);
      // }
      // alledges.push_back({p_edge.src, p_edge.dst, commit_version, true});
//        i++;
//        if (i % 1000 == 0) {
//        cout << ".";
//        cout.flush();
//        }
//      } catch (MultipleVersionException& e) {
//         NOP
//      }
    }

    for (auto [e, properties, properties_size] : edges_to_update_or_insert) {
      edge_t p_edge (ds->physical_id(e.src), ds->physical_id(e.dst));
      ds->insert_edge_version(p_edge, commit_version, properties, properties_size);
      // if(!ds->has_edge_version_p(p_edge, commit_version)){
      //   cout<<"\nedge not found after insertion\n";
      //   throw EdgeDoesNotExistsException(e);
      // }
    }
    // edge_t e(3931201, 4172466);
    // get_weight(e, nullptr);

//    cout << endl<< "done inserting" << endl;
    release_locks();
    return true;
  } catch (exception &e) {
    cout << "rolling back" << endl;
    rollback();
    release_locks();
    throw e;
  }
}

void SnapshotTransaction::register_precondition(Precondition *c) {
  vertex_id_t lock_to_aquire = c->requires_vertex_lock();
  if (lock_to_aquire != numeric_limits<vertex_id_t>::max()) {
    locks_to_aquire.push_back(lock_to_aquire);
  } else {
    for (vertex_id_t l : c->requires_vertex_locks()) {
      locks_to_aquire.push_back(l);
    }
  }
  preconditions.push_back(c);
}

void SnapshotTransaction::assert_preconditions() {
  for (auto p: preconditions) {
    p->assert_it(*ds, commit_version);
  }

}

void SnapshotTransaction::aquire_locks_and_insert_vertices() {
  sort(locks_to_aquire.begin(), locks_to_aquire.end());
  vertex_id_t last_lock = numeric_limits<vertex_id_t>::max();
  for (const auto &v : locks_to_aquire) {  // Relies on locks_to_aquire being a sorted data structure
    if (v != last_lock) {  // Dedup locks
      if (!ds->aquire_vertex_lock(v)) {
        if (find(vertices_to_insert.begin(), vertices_to_insert.end(), v) != vertices_to_insert.end()) {
          last_lock_aquired = v;
          if(ds->insert_vertex_version(v, commit_version)) {
            rollbacks.push_back(RollbackAction::generate_rollback_insert_vertex(v));
          } else if (!vertex_does_not_exists_semantic_activated ) {
            throw VertexExistsException(v);
          }
        } else {
          throw VertexDoesNotExistsException(v);
        }
      } else {
        last_lock_aquired = v;  // TODO needs to be initilized to another value than 0
      }
      last_lock = v;
    }
  }
}

void SnapshotTransaction::release_locks() {
  vertex_id_t last_lock = numeric_limits<vertex_id_t>::max();
  for (auto &v : locks_to_aquire) {  // Relies on locks_to_aquire being a sorted data structure
    if (v != last_lock && v <= last_lock_aquired) { // Dedup locks && do not release locks which have not been aquired.
        ds->release_vertex_lock(v);
        last_lock = v;
    } else if (v > last_lock_aquired) {
      cout << "did not release lock " << v << endl;
    }
  }
}

size_t SnapshotTransaction::vertex_count() {
  if (write_only) {
    throw IllegalOperation("Cannot read with a WriteOnly Transaction");
  }
  return number_of_vertices;
}

bool SnapshotTransaction::insert_vertex(vertex_id_t v) {
  locks_to_aquire.push_back(v);
  vertices_to_insert.push_back(v);
  return false;
}

bool SnapshotTransaction::delete_vertex(vertex_id_t v) {
  locks_to_aquire.push_back(v);
  vertices_to_delete.push_back(v);
  return false;
}

bool SnapshotTransaction::insert_edge(edge_t edge) {
  
  return insert_edge(edge, nullptr, 0);
}

bool SnapshotTransaction::delete_edge(edge_t edge) {
  locks_to_aquire.push_back(edge.src);
  edges_to_delete.push_back(edge);
  return false;
}

size_t SnapshotTransaction::neighbourhood_size_p(vertex_id_t src) {
  if (write_only) {
    throw IllegalOperation("Cannot read with a WriteOnly Transaction");
  }
  if(!m_analytics){
    ds->aquire_vertex_lock_shared_p(src);
  }
  auto ret = ds->neighbourhood_size_version_p(src, read_version);
  if(!m_analytics)
    ds->release_vertex_lock_shared_p(src);
  return ret;
}

void SnapshotTransaction::intersect_neighbourhood_p(vertex_id_t a, vertex_id_t b, vector<dst_t> &out) {
  if (write_only) {
    throw IllegalOperation("Cannot read with a WriteOnly Transaction");
  }
  if(!m_analytics){
    ds->aquire_vertex_lock_shared_p(min(a, b));
    ds->aquire_vertex_lock_shared_p(max(a, b));
  }
  
  ds->intersect_neighbourhood_version_p(a, b, out, read_version);
  if(!m_analytics){
    ds->release_vertex_lock_shared_p(a);
    ds->release_vertex_lock_shared_p(b);
  }
}

bool SnapshotTransaction::has_edge_p(edge_t edge) {
  if (write_only) {
    throw IllegalOperation("Cannot read with a WriteOnly Transaction");
  }
  
  if(!m_analytics){
    ds->aquire_vertex_lock_shared_p(edge.src);
  
  }
  
  auto ret = ds->has_edge_version_p(edge, read_version);
  if(!m_analytics)
  ds->release_vertex_lock_shared_p(edge.src);
  return ret;
}

void SnapshotTransaction::report_storage_size() {
  return ds->report_storage_size();
}

version_t SnapshotTransaction::get_version() const {
  return read_version;
}

SnapshotTransaction::~SnapshotTransaction() {
//  for (auto p : preconditions) {
//    delete p;
//  }
}

void SnapshotTransaction::bulkload(const SortedCSRDataSource &src) {
  for (auto v = 0u; v < ds->max_physical_vertex(); v++) {
    ds->aquire_vertex_lock(v);
  }
  ds->bulkload(src);
  for (auto v = 0u; v < ds->max_physical_vertex(); v++) {
    ds->release_vertex_lock(v);
  }
}

VersionedTopologyInterface *SnapshotTransaction::raw_ds() {
  return ds;
}

void SnapshotTransaction::clear(bool write_only) {
  this->write_only = write_only;
  read_version = NO_TRANSACTION;
  commit_version = NO_TRANSACTION;
  last_lock_aquired = 0;
  preconditions.clear();
  locks_to_aquire.clear();
  vertices_to_insert.clear();
  vertices_to_delete.clear();
  edges_to_insert.clear();
  edges_to_delete.clear();
}

bool SnapshotTransaction::has_vertex_p(vertex_id_t v) {
  if (write_only) {
    throw IllegalOperation("Cannot read with a WriteOnly Transaction");
  }
  //CAN WE AVOID LOCKING HERE????
  if(!m_analytics){
    ds->aquire_vertex_lock_shared_p(v);
  }
  bool ret = ds->has_vertex_version_p(v, read_version);
  if(!m_analytics){
    ds->release_vertex_lock_shared_p(v);
  }
  return ret;
}

size_t SnapshotTransaction::edge_count() {
  if (write_only) {
    throw IllegalOperation("Cannot read with a WriteOnly Transaction");
  }
  return ds->edge_count_version(read_version);
}

void SnapshotTransaction::use_vertex_does_not_exists_semantics() {
  vertex_does_not_exists_semantic_activated = true;
}

void SnapshotTransaction::use_edge_does_not_exists_semantics() {
  edge_does_not_exists_semantic_activated = true;
}

vertex_id_t SnapshotTransaction::physical_id(vertex_id_t v) {
  return ds->physical_id(v);
}

vertex_id_t SnapshotTransaction::logical_id(vertex_id_t v) {
  return ds->logical_id(v);
}

void SnapshotTransaction::assert_std_preconditions() {
  // Vertices of each edge to insert need to exists.
  for (auto [e, _, _1] : edges_to_insert) {
    // TODO is that to slow?
    if (!ds->has_vertex_version(e.src, commit_version) && find(vertices_to_insert.begin(), vertices_to_insert.end(), e.src) == vertices_to_insert.end()) {
      throw VertexDoesNotExistsException(e.src);
    }
    if (!ds->has_vertex_version(e.dst, commit_version) && find(vertices_to_insert.begin(), vertices_to_insert.end(), e.src) == vertices_to_insert.end()) {
      throw VertexDoesNotExistsException(e.dst);
    }
  }

  if (!vertex_does_not_exists_semantic_activated) {
    // Vertices to delete have to exists
    for (auto v : vertices_to_delete) {
      if (!ds->has_vertex_version(v, commit_version)) {
        throw VertexDoesNotExistsException(v);
      }
    }
  }

  if (!edge_does_not_exists_semantic_activated) {
    // New edges cannot exist already
    for (auto [e, _, _1] : edges_to_insert) {
      if (ds->has_edge_version(e, commit_version)) {
        edge_t p_edge(ds->physical_id(e.src), ds->physical_id(e.dst));
        ds->has_edge_version_p(p_edge, commit_version, true);
        throw EdgeExistsException(e);
      }
    }

    // Edges to delete have to existsgced_edges = 0;
// thread_local int VersioningBlockedSkipListAdjacencyList::gc_merges = 0;
// thread_local int VersioningBlockedSkipListAdjacencyList::gc_to_single_block = 0;
    for (auto e : edges_to_delete) {
      if (!ds->has_edge_version(e, commit_version)) {
        pair<int,int> tofind = make_pair(ds->physical_id(e.src), ds->physical_id(e.dst));
        

        cout<<"looking for edge "<<e.src<<" "<<e.dst<<" not found!!"<<endl;
        cout<<"physical id: "<<ds->physical_id(e.src)<<" "<<ds->physical_id(e.dst)<<endl;
        // for(int i=0;i<edges_to_delete.size();i++)
        //   cout<<edges_to_delete[i].src<<" "<<edges_to_delete[i].dst<<endl;

        edge_t p_edge (ds->physical_id(e.src), ds->physical_id(e.dst));
        cout<<"\n\ncommit version "<<commit_version<<endl;
        ds->has_edge_version_p(p_edge, commit_version, true);
        cout<<endl;
        ofstream output;
        output.open("debug.txt");
        for(int i=0;i<alledges.size();i++)
        {
          if(alledges[i].src == p_edge.src)
            output<<i<<" "<<alledges[i].src<<" "<<alledges[i].dst<<" "<<alledges[i].version<<" "<<alledges[i].insert<<endl;
        }
        cout<<endl<<endl;
        cout<<"\n\ncommit version "<<commit_version-1<<endl;

        ds->has_edge_version_p(p_edge, commit_version-1, true);
        cout<<endl<<endl;
        // cout<<"\n\ncommit version "<<commit_version-2<<endl;

        // ds->has_edge_version_p(p_edge, commit_version-2, true);
                cout<<endl<<endl;

        exit(0);

        throw EdgeDoesNotExistsException(e);
      }
    }
  }
}

void SnapshotTransaction::rollback() {
  for (auto rb : rollbacks) {
    switch (rb.type) {
      case (RollbackAction::INSERT_VERTEX): {
        ds->rollback_vertex_insert(rb.vertex);
        break;
      } default: {
        throw NotImplemented();
      }
    }
  }
}

size_t SnapshotTransaction::max_physical_vertex() {
  return max_physical_vertex_id;
}

bool SnapshotTransaction::insert_edge(edge_t edge, char *properties, size_t property_size) {
  locks_to_aquire.push_back(edge.src);
  edges_to_insert.emplace_back(edge, properties, property_size);
  return false;
}

VersionedBlockedEdgeIterator SnapshotTransaction::neighbourhood_blocked_p(vertex_id_t src) {
  if (write_only) {
    throw IllegalOperation("Cannot read with a WriteOnly Transaction");
  }

  // if(tm->get_epoch()>100)
  //   cout<<"read version being used "<<read_version<<" current epoch: "<<tm->get_epoch()<<endl<<endl;
  return ds->neighbourhood_version_blocked_p(src, read_version);
}

VersionedBlockedPropertyEdgeIterator SnapshotTransaction::neighbourhood_with_properties_blocked_p(vertex_id_t src) {
  if (write_only) {
    throw IllegalOperation("Cannot read with a WriteOnly Transaction");
  }
  return ds->neighbourhood_version_blocked_with_properties_p(src, read_version);
}


bool SnapshotTransaction::insert_or_update_edge(edge_t edge, char *properties, size_t property_size) {
  locks_to_aquire.push_back(edge.src);
  edges_to_update_or_insert.emplace_back(edge, properties, property_size);
  return true;
}

bool SnapshotTransaction::get_weight(edge_t edge, char *out) {
  if (write_only) {
    throw IllegalOperation("Cannot read with a WriteOnly Transaction");
  }
  
  if(!m_analytics){
    ds->aquire_vertex_lock(edge.src);
  }
  auto ret = ds->get_weight_version(edge, read_version, out);
  if(!m_analytics)
    ds->release_vertex_lock(edge.src);
  return ret;
}

bool SnapshotTransaction::get_weight_p(edge_t edge, char* out) {
  if (write_only) {
    throw IllegalOperation("Cannot read with a WriteOnly Transaction");
  }
  bool locked = false;
  if(!m_analytics){
      ds->aquire_vertex_lock_shared_p(edge.src);
  }
  auto ret = ds->get_weight_version_p(edge, read_version, out);
  if(!m_analytics)
    ds->release_vertex_lock_shared_p(edge.src);
  return ret;
}

void SnapshotTransaction::rewrite_inserted_vertex_timestamps() {
  // NOP as vertex versioning is not yet supported.
}

version_t SnapshotTransaction::get_commit_version() const {
  return commit_version;
}

void SnapshotTransaction::set_read_timestamp(version_t version) {
  read_version = version;

  max_physical_vertex_id = ds->max_physical_vertex();
  number_of_vertices = ds->vertex_count_version(read_version);
}

bool SnapshotTransaction::has_vertex(vertex_id_t v) {
  return ds->has_vertex_version(v, read_version);
}
