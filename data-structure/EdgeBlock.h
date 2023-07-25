//
// Created by per on 23.03.21.
//

#ifndef LIVE_GRAPH_TWO_EDGEBLOCK_H
#define LIVE_GRAPH_TWO_EDGEBLOCK_H

#include <cassert>
#include <cstring>
#include <utils/utils.h>
#include <iostream>
#include <utils/NotImplemented.h>
#include "AdjacencySetTypes.h"
#include "EdgeVersionRecord.h"
#include <functional>
inline version_t inline_version(bool deletion, bool more_versions, version_t version) {
  if (more_versions) {
    version |= MORE_VERSION_MASK;
  }
  if (deletion) {
    version |= DELETION_MASK;
  }
  return version;
}

inline version_t inline_unmask(version_t e){
  e &= ~(DELETION_MASK);
  return e;
}


/**
 * Represents a block of memory which contains edges, versions and properties.
 *
 * The block starts with edges, interleaved with versions and ends with properties. The edges and versions grow
 * upwards and the properties grow downwards.
 *
 * In the unversioned case, the property value belonging to an edge has the same offset in the property section.
 * In the versioned case, the property value belonging to an edge has the same offset in the property section minus all versions that are before the edge in question.
 * This allows random access to edge and property in the unversioned case but requires scanning edges from the beginning in the versioned case.
 *
 * We do not support multiple property versions yet. They can be supported the same way as supporting edge versions but requires to clean the property section on GC.
 */

/*
  removed attributes: end, edges, properties
  added propertes: edges, properties, weights
*/
class EdgeBlock {
public:
    EdgeBlock(dst_t *start_block, size_t _capacity, size_t _edges, size_t _property_size){
      capacity = _capacity; 
      property_size =_property_size; 
      edges = _edges;
      start = start_block;
      weights = (weight_t*)((char *)start + capacity*sizeof(dst_t));
      properties =  (char*)weights + capacity*sizeof(weight_t);
      
      std::cout<<"num edges: "<<edges<<std::endl;
      // std::cout<<"number of bytes for edges: "<<(char*)weights - (char*)start<<std::endl;
      // std::cout<<"number of bytes for weights: "<<(char*)properties - (char*)weights<<std::endl;

    }
              //will the pointer casting be done implicitly??
              //does the memory layout inside the block matter for locality?


    static EdgeBlock from_vskip_list_header(VSkipListHeader* header, size_t block_size, size_t property_size) {
      return EdgeBlock(header->data, block_size, header->size, property_size);
    };

    static EdgeBlock from_single_block(dst_t* start, size_t capacity, size_t edges, size_t property_size) {
      return EdgeBlock(start, capacity, edges, property_size);
    }

    bool has_space_to_insert_edge() {
      return edges + 1 <= get_block_capacity();
    };

    //TODO not needed
    bool has_space_to_delete_edge() {
      return edges + 1 <= get_block_capacity();
    }

    bool get_weight(dst_t e, char* out) {

      std::cout<<"edges: ";
      for(int i=0;i<edges;i++)
      std::cout<<*(start+i)<<" ";
      std::cout<<std::endl;

      dst_t *pos = find_upper_bound(start, edges, e);

      if(pos>=start+capacity) return false;
      
      if(inline_unmask(*pos) == e)
      {
        if(!is_deletion(*(pos)))
        {
          std::cout<<"edge found at position: "<<(pos-start)<<std::endl;
          int ind = pos-start;
          for(int i=0;i<property_size;i++)
          std::cout<<(int)(*(properties_start() + ind+i))<<" ";
          std::cout<<std::endl;
          memcpy(out, properties_start() + ind*property_size, property_size);
          return true;
        }
        else return false;
      }

      else return false;

    }

    /**
     * Finds the correct place to add edge, version record and properties and inserts them by shifthing.
     *
     * @param e
     * @param version
     * @param properties
     */
    bool insert_edge(dst_t e, weight_t weight, char *property) {
      
      assert(has_space_to_insert_edge());
      auto pos = find_upper_bound(start, edges, e);
      std::cout<<"Edge being inserted at position: "<<(pos-start)<<std::endl;
      if (pos == start + edges) {  // No version of this edge exists.
        *pos = e;
        *(weights+edges) = weight;
        memcpy(properties + edges*property_size, property, property_size);
      }
      else if(make_unversioned(*pos)!=e){
        int units_to_move = edges - (pos - start); 
        int new_pos = pos-start;
        memmove(pos+1, pos, units_to_move*sizeof(dst_t));
        memmove(weights + new_pos+1, weights + new_pos, units_to_move*sizeof(weight_t));
        memmove(properties + (new_pos + 1)*property_size ,properties + property_size*new_pos, units_to_move*property_size);

        *pos = e;
        *(weights + new_pos) = weight;
        memcpy(properties + new_pos*property_size, property, property_size);
      }

      else {
        int new_pos = pos-start;
        *pos = e;
        *(weights + new_pos) = weight;
        memcpy(properties + new_pos*property_size, property, property_size);
      }
      edges+=1;
      return true;

    };


    bool delete_edge(dst_t e) {
      // assert(has_space_to_delete_edge());

      auto ptr = find_upper_bound(start, edges, e);
      if (ptr == start + edges || make_unversioned(*ptr) != e) {  // Edge does not exist
        return false;
      } 
      else{
        int new_pos = ptr - start;
        memmove(start + new_pos, start + new_pos + 1, (edges-new_pos-1)*sizeof(dst_t));
        memmove(weights + new_pos, weights + new_pos + 1, (edges-new_pos-1)*sizeof(weight_t));
        memmove(properties + new_pos*property_size, properties + (new_pos + 1)*property_size, (edges-new_pos-1)*property_size);
      }
      edges-=1;
    }

    /**
     * MAY NOT NEED THIS FUNCTION
     */
    bool gc(version_t min_version, const vector<version_t>& sorted_active_versions) {
      // Removes unncessary versions and shifts remaining destinations and versions forward.
      auto shift = 0; // The forward shift to use, increases when versions are removed.
      bool version_remaining = false;
      size_t new_size = edges;

      // Tracks how many edges we encountered and left so far, with out counting versions and deleted edges
      auto edges_so_far = 0;
      for (auto i = start; i < start + edges; i++) {
        auto e = *i;
        auto v = *(i+1);

        // Prune version chain and inline it if possible.
        if (is_versioned(e)) {
          EdgeVersionRecord vr {make_unversioned(e), (version_t*) i+1, nullptr, false, property_size};
          vr.gc(min_version, sorted_active_versions);
          v = *(i+1);
        }

        // Remove inline versions
        if (is_versioned(e) && !(v & MORE_VERSION_MASK) && timestamp(v) < min_version) {
          if (is_deletion(v)) {
            new_size -= 2;
            shift += 2;
            i += 1;

            // Removes property by moving all properties before by 1
            memmove(properties_start() + property_size, properties_start(), edges_so_far * property_size);
            properties -= 1;
          } else {
            *(i - shift) = make_unversioned(e);
            new_size -= 1;
            shift += 1;
            i += 1;
            edges_so_far += 1;
          }
        } else {  // Version cannot be removed or is not versioned.
          if (is_versioned(e)) {
            version_remaining = true;
            *(i - shift) = e;
            *(i - shift + 1) = v;
            edges_so_far += 1;
            i += 1;
          } else {
            *(i - shift) = e;
            edges_so_far += 1;
          }
        }
      }
      edges = new_size;
      return version_remaining;

    };

    void copy_into(EdgeBlock &other) {
      assert(size() <= other.size());

      other.set_edges(edges);
      other.set_property_size(property_size);

      memcpy(other.start, start, edges * sizeof(dst_t));
      memcpy(other.properties_start(), properties, edges * property_size);
      memcpy(other.weights_start(), weights, edges*sizeof(weight_t));
    };

    size_t split_into(EdgeBlock &other) {
      auto split = edges / 2;

      
      memcpy(other.start, start + split, (edges - split) * sizeof(dst_t));

      // Copy properties into new block.
      memcpy(other.properties_start(), properties, (edges - split)* property_size);
      // Copy weights into new block
      memmove(other.weights_start(), weights, (edges - split) * sizeof(weight_t));

      other.edges = edges - split;
      edges = split;
      other.property_size = property_size;

      return split;
    }

    /**
     * Moves `elements` number of edges and versions from b1 to b2.
     * If b1 and b2 are sorted internally and b1.max < b2.min, then the condition holds afterwards.
     *
     * If b1[elements] is a versioned edge, it moves one element more to keep edge and version together.
     */
    static void move_forward(EdgeBlock& from, EdgeBlock& to, size_t elements) {
      assert(from.edges >= elements);
      assert(from.get_max_edge() < to.get_min_edge());
      assert(to.property_size == from.property_size);
      assert(to.edges + elements <= to.capacity);
      auto property_size = to.property_size;

      auto elements_not_to_move = from.edges - elements;

//#ifdef DEBUG
//      vector<dst_t> before;
//      for (auto i = from.start; i < from.start + from.edges; i++) {
//        before.emplace_back(*i);
//      }
//
//      for (auto i = to.start; i < to.start + to.edges; i++) {
//        before.emplace_back(*i);
//      }
//#endif

      // Move elements in to backwards to make place
      memmove((char*) (to.start + elements), (char*) to.start, to.edges * sizeof(dst_t));
      memmove((char*) (to.weights_start() + elements), (char*) to.weights_start(), to.edges * sizeof(weight_t));
      memmove((char*) (to.properties_start() + to.property_size*elements), (char*) to.properties_start(), to.edges * to.property_size);
      // Move elements from to to from
      memcpy((char*) to.start, (char*) (from.start + elements_not_to_move), elements * sizeof(dst_t));
      memcpy((char*) to.weights_start(), (char*) (from.weights_start() + elements_not_to_move), elements * sizeof(weight_t));
      memcpy((char*) to.properties_start(), (char*) (from.properties_start() + from.property_size*elements_not_to_move), elements * property_size);

      // Move Properties from to to from

      from.edges -= elements;
      to.edges += elements;

//#ifdef DEBUG
//      vector<dst_t> after;
//      for (auto i = from.start; i < from.start + from.edges; i++) {
//        after.emplace_back(*i);
//      }
//      for (auto i = to.start; i < to.start + to.edges; i++) {
//        after.emplace_back(*i);
//      }
//
//      bool matching = before == after;
//      if (!matching) {
//        cout << from.edges << " " << to.edges << endl;
//        cout << "Moved " << elements << " elements";
//        cout << "Content before" << endl;
//        for (auto i = 0; i < before.size(); i++) {
//          if (i == elements_not_to_move) {
//
//          }
//          cout << before[i] << " ";
//        }
//        cout << endl;
//        cout << "Content after:" << endl;
//        for (auto i = 0; i < before.size(); i++) {
//          cout << after[i] << " ";
//        }
//        cout << endl;
//        assert(false);
//      }
//#endif
    }

    static void move_backward(EdgeBlock& from, EdgeBlock& to, size_t elements) {
      assert(from.edges >= elements);
      assert(from.get_min_edge() > to.get_max_edge());
      assert(to.property_size == from.property_size);
      assert(to.edges + elements <= to.capacity);

      auto property_size = to.property_size;

      // Move elements from to to from
      memcpy((char*) (to.start + to.edges), (char*) from.start, elements * sizeof(dst_t));
      memcpy((char*) (to.weights_start() + to.edges), (char*) from.weights_start(), elements * sizeof(weight_t));
      memcpy((char*) (to.properties_start() + to.edges*property_size), (char*) from.start, elements * property_size);
      // Move elements in from backwards
      memmove((char*) from.start, (char*) (from.start + elements), (from.edges - elements) * sizeof(dst_t));
      memmove((char*) from.weights_start(), (char*) (from.weights_start() + elements), (from.edges - elements) * sizeof(weight_t));
      memmove((char*) from.properties_start(), (char*) (from.properties_start() + elements*property_size), (from.edges - elements) * property_size);

      from.edges -= elements;
      to.edges += elements;
    }

    size_t count_edges() {
      return edges;
    }

    dst_t* get_single_block_pointer() {
      return start;
    }

    //redundant, same as count_edges
    size_t get_edges() {
      return edges;
    }

    size_t get_block_capacity() {
      return capacity;
    }

    void set_edges(size_t num_edges){
      edges = num_edges;
    } 

    void set_property_size(size_t property_size){
      this->property_size = property_size; 
    }

    dst_t get_max_edge() {
      return *(start + edges);
    }

    dst_t get_min_edge() {
      return *start;
    }


    //to be looked into
    void update_skip_list_header(VSkipListHeader *h) {
      h->size = edges;
      h->max = get_max_edge();
    }

    char *properties_start() {
      return properties;
    }

    weight_t *weights_start(){
      return weights;
    }

    char *properties_end() {
      return end;
    }

    /**
   * Start of the memory region
   */
    dst_t *start;

    void print_block(function<dst_t(dst_t)> physical_to_logical) {
      cout << "Physical Edges: " << endl;
      for (auto i = start; i < start + edges; i++) {
        cout << *i <<" ";
      }
      cout << endl;
      cout << "Logical Edges: " << endl;
      for (auto i = start; i < start + edges; i++) {
        auto e = *i;
        cout << " " << physical_to_logical(e);
      }
      cout << endl;
      cout << "Weights: " << endl;
      for (weight_t* i = (weight_t*) weights_start(); i < (weight_t*) properties_start() + edges; i++) {
        cout << " " << *i;
      }
      cout << endl;

      cout << "Properties: " << endl;
      auto j = properties_start(), properties = properties_start();
      for (int i=0;i<edges;i++) {
        for(;j<properties + (i+1)*property_size;j++)
        { 
          if(*j) cout<< *j; 
          else break;
        }  
        cout<<endl;
      }
    }

     /**
     * Finds the upper bound for value in a sorted array.
     *
     * Ignores versions.
     *
     * @param start
     * @param end
     * @param value
     * @return a pointer to the position of the upper bound or end.
     * @return a pointer to the position of the upper bound or end.
     */
    static dst_t* find_upper_bound(dst_t *start, int edges, dst_t value) {
     int l=0, r=edges;
      int mid = (l+r)/2;
      while(r-l>1){
        if(inline_unmask(*(start+mid))>=value) r=mid;
        else l=mid;
        mid = (l+r)/2;
      }

      if(inline_unmask(*(start+mid))>=value)
        return start+mid;
      return start+r;
    }

    /**
    * End of the memory region.
    */
    char* end;

private:
    size_t capacity;



    /**
     * The number of edges.
     */

    size_t edges;
    /**
     * Start of properties
     */
    char *properties;
    weight_t *weights;

    /**
     * The size in bytes of each property.
     */
    size_t property_size;

    size_t size() {
      return (end - (char*) start);
    };



    //MAY NOT BE NEEDED

    void insert_properties_by_shift(char *properties, size_t offset) {
      if (offset == 0) {
        memcpy(properties_start() - property_size, properties, property_size);
      } else {
        memmove(properties_start() - property_size, properties_start(), offset * property_size);
        memcpy(properties_start() + (offset - 1) * property_size, properties, property_size);
      }
    }

    //MAY NOT BE NEEDED
    size_t count_versions_before(size_t offset) {
      size_t versions = 0;
      for (uint i = 0; i < offset; i++) {
        if (is_versioned(start[i])) {
          versions += 1;
          i++; // Skip the version
        }
      }
      return versions;
    };
};



#endif //LIVE_GRAPH_TWO_EDGEBLOCK_H
