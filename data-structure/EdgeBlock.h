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
  // cout<<"received e = "<<e<<endl;
  e &= ~(DELETION_MASK);
  // cout<<"returning e = "<<e<<endl;
  return e;
}

inline bool isDeleted(dst_t dst){
  return dst & DELETION_MASK;
}

inline dst_t mark_deleted(dst_t dst){
  return DELETION_MASK | dst;
}

inline void setbit(uint64_t *bitmask, int pos){
  bitmask = (bitmask + pos/64);
  *(bitmask) |= (1uL << (63-pos%64));
}

inline void unsetbit(uint64_t *bitmask, int pos){
  bitmask = (bitmask + pos/64);
  *(bitmask) &= ~(1uL << (63-pos%64));
}

inline bool isBitSet(uint64_t *bitmask, int pos){
  int index = pos/64;
  int offset = (63 - pos%64);
  return *(bitmask + index) & (1ul << offset);
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
      end = (char*)properties + _property_size*_capacity;

      bitmask = (uint64_t*)end;
      // std::cout<<"number of bytes for bitmask: "<<(char*)start_block - (char*)bitmask<<std::endl;
      // std::cout<<"number of bytes for edges: "<<(char*)weights - (char*)start<<std::endl;
      // std::cout<<"number of bytes for weights: "<<(char*)properties - (char*)weights<<std::endl;
      if(_edges==0) 
      {
        for(int i=0;i<max(1ul,capacity/64);i++)
          *(bitmask + i) = 0;
      }
      if(edges==0)
        memset(start, 128, sizeof(dst_t)*capacity);

      // cout<<"edge block created successfully"<<endl;
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

      // std::cout<<"edges: ";
      // for(int i=0;i<edges;i++)
      // std::cout<<*(start+i)<<" ";
      // std::cout<<std::endl;

      dst_t *pos = find_upper_bound(start, get_max_edge_index(), e);

      if(pos>=start+capacity) return false;
      
      if(inline_unmask(*pos) == e)
      {
        if(!is_deletion(*(pos)))
        {
          // std::cout<<"edge found at position: "<<(pos-start)<<std::endl;
          int ind = pos-start;
          // for(int i=0;i<property_size;i++)
          // std::cout<<(int)(*(properties_start() + ind+i))<<" ";
          // std::cout<<std::endl;
          memcpy(out, properties_start() + ind*property_size, property_size);
          return true;
        }
        else return false;
      }

      else return false;

    }

    int findUnsetBitLeft(int new_pos){
      
      for(int i=new_pos-1;i>=((new_pos-1)/64)*64;i--)
      {
        bool isSet = isBitSet(bitmask, i);
        if(!isSet) return i;
      }

      for(int i=(new_pos-1)/64-1;i>=0;i--){
        if(*(bitmask+i)){
          for(int j=0;j<63;j++)
            if(*(bitmask+i) & (1ul<<j))
              return (i*64 + (63-j));
        }
      }


      return -1e9;
    }

    int findUnsetBitRight(int new_pos){
      
      for(int i=new_pos+1;i<((new_pos+1)/64+1)*64;i++)
      {
         bool isSet = isBitSet(bitmask, i);
        if(!isSet) return i;
      }
      for(int i=((new_pos+1)/64+1);i<max(1ul, capacity/64);i++)
      {
        if(*(bitmask+i)){
          for(int j=63;j>=0;j--)
            if(*(bitmask+i) & (1ul<<j))
              return (i*64 + (63-j));
        }
      }

      return 1e9;
    } 

    /**
     * Finds the correct place to add edge, version record and properties and inserts them by shifting.
     *
     * @param e
     * @param version
     * @param properties
     */
    bool insert_edge(dst_t e, weight_t weight, char *property) {
      
      assert(has_space_to_insert_edge());
      auto pos = find_upper_bound(start, get_max_edge_index(), e);
      int new_pos = pos-start;
      // std::cout<<"Edge being inserted at position: "<<(pos-start)<<std::endl;
      if(!isBitSet(bitmask, new_pos)){
        // cout<<"can insert edge here at pos: "<<new_pos<<endl;
      }
      else if(inline_unmask(*pos)!=e){
        
        int left = findUnsetBitLeft(new_pos);
        int right = findUnsetBitRight(new_pos);

        if(abs(left - new_pos) > abs(right - new_pos)){
           int units_to_move = right - new_pos; 
            setbit(bitmask, right);
           memmove(pos+1, pos, units_to_move*sizeof(dst_t));
           memmove(weights + new_pos+1, weights + new_pos, units_to_move*sizeof(weight_t));
           memmove(properties + (new_pos + 1)*property_size ,properties + property_size*new_pos, units_to_move*property_size);
        }

        else{
          setbit(bitmask, left);
          int units_to_move = new_pos - left; 

          memmove(start + left, start + left + 1, units_to_move*sizeof(dst_t));
          memmove(weights + left, weights + left + 1, units_to_move*sizeof(weight_t));
          memmove(properties + (left)*property_size ,properties + property_size*(left+1), units_to_move*property_size);

        }
        
      }

      *pos = e;
      *(weights + new_pos) = weight;
      memcpy(properties + new_pos*property_size, property, property_size);
        // cout<<"inserted edge now setting bit"<<endl;
        setbit(bitmask, new_pos);
      edges+=1;
      return true;

    };


    bool delete_edge(dst_t e) {
      // assert(has_space_to_delete_edge());

      auto ptr = find_upper_bound(start, get_max_edge_index(), e);
      if (ptr == start + get_max_edge_index() || make_unversioned(*ptr) != e || isDeleted(*ptr)) {  // Edge does not exist
        return false;
      } 
      else{

        *ptr = mark_deleted(*ptr);
        int pos = ptr - start;
        unsetbit(bitmask, pos);

        // if(edges>1){
        //     int new_pos = ptr - start;
        //     memmove(start + new_pos, start + new_pos + 1, (edges-new_pos-1)*sizeof(dst_t));
        //     memmove(weights + new_pos, weights + new_pos + 1, (edges-new_pos-1)*sizeof(weight_t));
        //     memmove(properties + new_pos*property_size, properties + (new_pos + 1)*property_size, (edges-new_pos-1)*property_size);
        // }
        // else{ std::cout<<"didnt delete edge"<<std::endl;}
      }
      edges-=1;
      return true;
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
      // assert(size() <= other.size());
      // assert(capacity<=other.capacity);
      assert(edges <= other.capacity);
      other.set_edges(edges);
      other.set_property_size(property_size);

      for(int i=0;i<max(1ul, other.capacity/64);i++)
        *(other.bitmask_start()+i) = 0;

      memcpy(other.bitmask_start(), bitmask, max(1ul,capacity/64)*sizeof(uint64_t));
      memcpy(other.start, start, edges * sizeof(dst_t));
      memcpy(other.properties_start(), properties, edges * property_size);
      memcpy(other.weights_start(), weights, edges*sizeof(weight_t));

      cout<<"other block after copying"<<endl;

      for(int i=0;i<edges;i++)
        cout<<*(other.start+i)<<" ";
      cout<<endl;
    };

    size_t split_into(EdgeBlock &other) {
      auto split = edges / 2;
      
      
      memcpy(other.start, start + split, (edges - split) * sizeof(dst_t));

      // Copy properties into new block.
      memcpy(other.properties_start(), properties + split*property_size, (edges - split)* property_size);
      // Copy weights into new block
      memmove(other.weights_start(), weights + split, (edges - split) * sizeof(weight_t));

      other.edges = edges - split;
      edges = split;
      other.property_size = property_size;

      for(int i=capacity/128;i<capacity/64;i++)
        *(bitmask + i) = 0;
      for(int i=0;i<capacity/128;i++){
        *(other.bitmask+i) = ~(0ul);

      }

      return split;
    }

    /**
     * Moves `elements` number of edges and versions from b1 to b2.
     * If b1 and b2 are sorted internally and b1.max < b2.min, then the condition holds afterwards.
     *
     * If b1[elements] is a versioned edge, it moves one element more to keep edge and version together.
     */
    
    static void compactBlock(EdgeBlock& block){
      uint64_t *bitmask = block.bitmask_start();
      dst_t* start = block.start;
      weight_t* weights_start = block.weights_start();
      char* properties_start = block.properties_start();
      int current = 0; bool found = false;
      cout<<"testin compact block: ";
      for(int i=0;i<block.capacity;i++) cout<< *(block.start+i)<<" ";
      cout<<endl;
      for(int i=0;i<block.capacity;i++){
        if(isBitSet(bitmask, i))
        {
          if(current != i){
            memcpy(start+current, start + i, sizeof(dst_t));
            memcpy(weights_start+current, weights_start + i, sizeof(weight_t));
            memcpy(properties_start+(current*block.property_size), properties_start+(i*block.property_size), block.property_size);
            unsetbit(bitmask, i);
            setbit(bitmask, current);
            current++;
          }
          else current++;
        }
      }
      cout<<"block after compaction: "<<endl;
      for(int i=0;i<block.capacity;i++)
        cout<< *(block.start+i)<<" ";
      cout<<endl;
    }
    
    static void move_forward(EdgeBlock& from, EdgeBlock& to, size_t elements) {


      assert(from.edges >= elements);
      assert(from.get_max_edge() < to.get_min_edge());
      assert(to.property_size == from.property_size);
      assert(to.edges + elements <= to.capacity);
      
      compactBlock(to);
      compactBlock(from);
      
      auto property_size = to.property_size;

      auto elements_not_to_move = from.edges - elements;

      // Move elements in to backwards to make place
      memmove((char*) (to.start + elements), (char*) to.start, to.edges * sizeof(dst_t));
      memmove((char*) (to.weights_start() + elements), (char*) to.weights_start(), to.edges * sizeof(weight_t));
      memmove((char*) (to.properties_start() + to.property_size*elements), (char*) to.properties_start(), to.edges * to.property_size);

      for(int i=to.edges;i<to.edges+elements;i++)
        setbit(to.bitmask, i);


      // Move elements from to to from
      memcpy((char*) to.start, (char*) (from.start + elements_not_to_move), elements * sizeof(dst_t));
      memcpy((char*) to.weights_start(), (char*) (from.weights_start() + elements_not_to_move), elements * sizeof(weight_t));
      memcpy((char*) to.properties_start(), (char*) (from.properties_start() + from.property_size*elements_not_to_move), elements * property_size);
      for(int i=elements_not_to_move;i<from.edges;i++)
        unsetbit(from.bitmask,i);
      // Move Properties from to to from

      from.edges -= elements;
      to.edges += elements;

      
      
    }

    static void move_backward(EdgeBlock& from, EdgeBlock& to, size_t elements) {

      // vector<int> alledges;
      // for(int i=0;i<to.edges;i++)
      //   alledges.push_back(*(to.start+i));
      // for(int i=0;i<from.edges;i++)
      //   alledges.push_back(*(from.start+i));


      assert(from.edges >= elements);
      assert(from.get_min_edge() > to.get_max_edge());
      assert(to.property_size == from.property_size);
      assert(to.edges + elements <= to.capacity);

      compactBlock(to);
      compactBlock(from);

      auto property_size = to.property_size;

      // Move elements from from to to
      memcpy((char*) (to.start + to.edges), (char*) from.start, elements * sizeof(dst_t));
      memcpy((char*) (to.weights_start() + to.edges), (char*) from.weights_start(), elements * sizeof(weight_t));
      memcpy((char*) (to.properties_start() + to.edges*property_size), (char*) from.properties_start(), elements * property_size);
      for(int i=to.edges;i<to.edges+elements;i++)
        setbit(to.bitmask, i);
      
      // Move elements in from backwards
      memmove((char*) from.start, (char*) (from.start + elements), (from.edges - elements) * sizeof(dst_t));
      memmove((char*) from.weights_start(), (char*) (from.weights_start() + elements), (from.edges - elements) * sizeof(weight_t));
      memmove((char*) from.properties_start(), (char*) (from.properties_start() + elements*property_size), (from.edges - elements) * property_size);
      for(int i=0;i<elements;i++)
        unsetbit(from.bitmask, from.edges-1-i);


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


    //TO DO CHANGE THIS TO ACCOUNT TOMBSTONES
    dst_t get_max_edge() {
      for(int i=max(1ul, capacity/64);i>=0;i--)
      {
        if(*(bitmask+i)!=0){
          for(int j=0;j<64;j++)
            if(*(bitmask+i) & (1ul<<j)) 
              return *(start + i*64 + (63-j));
        }
      }
    }

    int get_max_edge_index(){
      for(int i=max(1ul, capacity/64)-1;i>=0;i--)
      {
        if(*(bitmask+i)!=0){
          for(int j=0;j<64;j++)
            if(*(bitmask+i) & (1ul<<j)) 
              return (i*64 + (63-j)+1);
            
        }
      }
      return 0;
    }

    dst_t get_min_edge() {
      for(int i=0;i<max(1ul, capacity/64);i++)
      {
        if(*(bitmask+i)!=0){
          for(int j=63;j>=0;j--)
            if(*(bitmask+i) & (1ul<<j)) 
              return *(start + i*64 + (63-j));
        }
      }
      return *start;
    }


    //to be looked into
    void update_skip_list_header(VSkipListHeader *h) {
      h->size = edges;
      dst_t max=0;
      for(int i=0;i<edges;i++)
      if(*(start+i)>max) max=*(start+i);
      // std::cout<<"actual max = "<<max<<" max returned: "<<get_max_edge()<<std::endl;
      h->max = get_max_edge();
    }

    char *properties_start() {
      return properties;
    }

    weight_t *weights_start(){
      return weights;
    }

    uint64_t* bitmask_start(){
      return bitmask;
    }

    char *properties_end() {
      return end;
    }

    void print_bitset(){
      cout<<endl<<"bitmask: ";
      for(int i=0;i<max(1ul, capacity/64);i++)
      {
        for(int j=63;j>=0;j--)
        if(*(bitmask+i) & (1ul<<j)) cout<<1;
        else cout<<0;

        cout<<" ";
      }

      cout<<endl;
    }

    /**
   * Start of the memory region
   */
    dst_t *start;

    void print_block(function<dst_t(dst_t)> physical_to_logical) {
      if(edges==0){
        cout<<"\n***Block Empty***\n";
        return;
      }
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
    static dst_t* find_upper_bound(dst_t *start, int edges, dst_t value, bool debug=false) {
         
     int l=0, r=edges;
      int mid = (l+r)/2;
      while(r-l>1){

        if(debug)
          cout<<"[eb find debug]: "<<l<<" "<<r<<" "<<mid<<" "<<inline_unmask(*(start+mid))<<endl;
        if(inline_unmask(*(start+mid))>=value) r=mid;
        else l=mid;
        mid = (l+r)/2;
        
          
      }

      if(inline_unmask(*(start+mid))>=value){
        // cout<<"returning start+mid"<<" "<<mid<<" "<<l<<" "<<r<<endl;
        return start+mid;
      }
      return start+r;
    }

    /**
    * End of the memory region.
    */
    char* end;
  size_t capacity;
private:
    



    /**
     * The number of edges.
     */

    size_t edges;
    /**
     * Start of properties
     */
    char *properties;
    weight_t *weights;
    uint64_t* bitmask;
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
