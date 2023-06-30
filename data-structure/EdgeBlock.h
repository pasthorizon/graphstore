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
class EdgeBlock {
public:
    EdgeBlock(dst_t *start, size_t capacity, size_t edges_and_versions, size_t properties, size_t property_size)
            : start(start), end(((char*) start) + capacity * sizeof(dst_t) + capacity * property_size), capacity(capacity), edges_and_versions(edges_and_versions), properties(properties),
              property_size(property_size) {};


    static EdgeBlock from_vskip_list_header(VSkipListHeader* header, size_t block_size, size_t property_size) {
      return EdgeBlock(header->data, block_size, header->size, header->properties, property_size);
    };

    static EdgeBlock from_single_block(dst_t* start, size_t capacity, size_t edges_and_versions, size_t properties, size_t property_size) {
      return EdgeBlock(start, capacity, edges_and_versions, properties, property_size);
    }

    // TODO move methods definitions to cpp file

    bool has_space_to_insert_edge() {
      return edges_and_versions + 2 <= get_block_capacity();
    };

    bool has_space_to_delete_edge() {
      return edges_and_versions + 1 <= get_block_capacity();
    }

    bool get_weight(dst_t e, version_t v, char* out) {
      // Using binary search would be faster if the block is not versioned.
      // This requires per block tracking of versioning.
      dst_t* e_ptr = nullptr;
      version_t* v_ptr = nullptr;
      auto property_offset = -1;
      auto version_count = 0;
      for (auto i = start; i < start + edges_and_versions; i++) {
        if (make_unversioned(*i) == e) {
          e_ptr = i;
          if (is_versioned(*i)) {
            v_ptr = (version_t*) (i + 1);
          }
          property_offset = i - start - version_count;
          break;
        }
        if (is_versioned(*i)) {
          version_count += 1;
          i++;
        }
      }
      if (e_ptr == nullptr) {
        return false;
      }
      version_t dummy_version = FIRST_VERSION;
      EdgeVersionRecord vr {make_unversioned(*e_ptr), v_ptr == nullptr ? &dummy_version : v_ptr, properties_start() + property_offset * property_size, true, property_size};
      if (vr.exists_in_version(v)) {
        auto w = vr.get_weight(v);
        memcpy(out, (char*) &w, property_size);
        return true;
      } else {
        return false;
      }
    }

    /**
     * Finds the correct place to add edge, version record and properties and inserts them by shifthing.
     *
     * @param e
     * @param version
     * @param properties
     */
    bool insert_edge(dst_t e, version_t version, char *properties) {
      assert(has_space_to_insert_edge());

      auto pos = find_upper_bound(start, start + edges_and_versions, e);
      if (pos == start + edges_and_versions || make_unversioned(*pos) != e) {  // No version of this edge exists.
        memmove((char*) (pos + 2), (char*) pos, (edges_and_versions - (pos - start)) * sizeof(dst_t));
        *pos = make_versioned(e);
        *(pos + 1) = inline_version(false, false, version);

        auto offset = pos - start;
        offset -= count_versions_before(offset);

        insert_properties_by_shift(properties, offset);
        edges_and_versions += 2;
        this->properties += 1;
      } else { // Earlier version of this edge exists and pos points to it.
        int offset = pos - start;
        int property_offset = offset - count_versions_before(offset);
        char* property = properties_start() + property_offset * property_size;
        EdgeVersionRecord vr {make_unversioned(*pos), pos + 1, property, true, property_size};
        if (vr.exists_in_version(version)) {
          vr.write(version, UPDATE, properties);
        } else {
          vr.write(version, INSERTION, properties);
        }

      }
      return true;
    };


    bool delete_edge(dst_t e, version_t version) {
      assert(has_space_to_delete_edge());

      auto ptr = find_upper_bound(start, start + edges_and_versions, e);
      if (ptr == start + edges_and_versions || make_unversioned(*ptr) != e) {  // Edge does not exist
        return false;
      } else if (is_versioned(*ptr)) {
        int offset = ptr - start;
        int property_offset = offset - count_versions_before(offset);
        char* property = properties_start() + property_offset * property_size;
        EdgeVersionRecord vr {make_unversioned(*ptr), ptr + 1, property, true, property_size};
        vr.write(version, DELETION, nullptr);
        return true;
      } else {
        memmove((char*) (ptr + 2), ptr + 1, (start + edges_and_versions - ptr - 1) * sizeof(dst_t));
        *ptr |= VERSION_MASK;
        *(ptr + 1) = version | DELETION_MASK;
        edges_and_versions += 1;
        return true;
      }
    }

    /**
     * Removes all version records < min_version.
     * @param min_version
     */
    bool gc(version_t min_version, const vector<version_t>& sorted_active_versions) {
      // Removes unncessary versions and shifts remaining destinations and versions forward.
      auto shift = 0; // The forward shift to use, increases when versions are removed.
      bool version_remaining = false;
      size_t new_size = edges_and_versions;

      // Tracks how many edges we encountered and left so far, with out counting versions and deleted edges
      auto edges_so_far = 0;
      for (auto i = start; i < start + edges_and_versions; i++) {
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
      edges_and_versions = new_size;
      return version_remaining;

    };

    void copy_into(EdgeBlock &other) {
      assert(size() <= other.size());

      other.edges_and_versions = edges_and_versions;
      other.properties = properties;
      other.property_size = property_size;

      memcpy(other.start, start, edges_and_versions * sizeof(dst_t));
      memcpy(other.properties_start(), properties_start(), properties * property_size);
    };

    tuple<size_t, size_t> split_into(EdgeBlock &other) {
      auto split = edges_and_versions / 2;

      if (is_versioned(start[split-1])) { // Keep the versioned edge together with its version.
        split -= 1;
      }
      memcpy(other.start, start + split, (edges_and_versions - split) * sizeof(dst_t));

      auto property_split = split - count_versions_before(split);
      auto properties_to_move = properties - property_split;

      // Copy properties into new block.
      memcpy((char*) other.end - properties_to_move * property_size, (char*) end - properties_to_move * property_size, properties_to_move * property_size);
      // Move properties in existing block
      memmove(end - property_split * property_size, properties_start(), property_split * property_size);

      other.edges_and_versions = edges_and_versions - split;
      other.properties = properties - property_split;
      edges_and_versions = split;
      properties = property_split;

      return {split, property_split};
    }

    /**
     * Moves `elements` number of edges and versions from b1 to b2.
     * If b1 and b2 are sorted internally and b1.max < b2.min, then the condition holds afterwards.
     *
     * If b1[elements] is a versioned edge, it moves one element more to keep edge and version together.
     */
    static void move_forward(EdgeBlock& from, EdgeBlock& to, size_t elements) {
      assert(from.edges_and_versions >= elements);
      assert(from.get_max_edge() < to.get_min_edge());
      assert(to.property_size == from.property_size);
      auto property_size = to.property_size;

      auto elements_not_to_move = from.edges_and_versions - elements;
      // Keep version and edge together.
      if (is_versioned(from.start[elements_not_to_move-1])) {
        assert(to.edges_and_versions + elements + 1 <= to.capacity);
        elements += 1;
        elements_not_to_move -=1;
      }

      auto versions_before = from.count_versions_before(elements_not_to_move);
      auto properties_not_to_move = elements_not_to_move - versions_before;
      auto properties_to_move = from.properties - properties_not_to_move;

//#ifdef DEBUG
//      vector<dst_t> before;
//      for (auto i = from.start; i < from.start + from.edges_and_versions; i++) {
//        before.emplace_back(*i);
//      }
//
//      for (auto i = to.start; i < to.start + to.edges_and_versions; i++) {
//        before.emplace_back(*i);
//      }
//#endif

      // Move elements in to backwards to make place
      memmove((char*) (to.start + elements), (char*) to.start, to.edges_and_versions * sizeof(dst_t));

      // Move elements from to to from
      memcpy((char*) to.start, (char*) (from.start + elements_not_to_move), elements * sizeof(dst_t));

      // Move Properties from to to from
      memcpy(to.properties_start() - properties_to_move * property_size, from.properties_start() + properties_not_to_move * property_size, properties_to_move * property_size);

      // Move properties in from to the end
      memmove(from.end - properties_not_to_move * property_size, from.properties_start(), properties_not_to_move * property_size);

      from.edges_and_versions -= elements;
      from.properties -= properties_to_move;
      to.edges_and_versions += elements;
      to.properties += properties_to_move;

//#ifdef DEBUG
//      vector<dst_t> after;
//      for (auto i = from.start; i < from.start + from.edges_and_versions; i++) {
//        after.emplace_back(*i);
//      }
//      for (auto i = to.start; i < to.start + to.edges_and_versions; i++) {
//        after.emplace_back(*i);
//      }
//
//      bool matching = before == after;
//      if (!matching) {
//        cout << from.edges_and_versions << " " << to.edges_and_versions << endl;
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
      assert(from.edges_and_versions >= elements);
      assert(from.get_min_edge() > to.get_max_edge());
      assert(to.property_size == from.property_size);
      auto property_size = to.property_size;

      // Keep version and edge together.
      if (is_versioned(from.start[elements - 1])) {
        assert(to.edges_and_versions + elements + 1 <= to.capacity);
        elements += 1;
      }

      auto versions_to_move = from.count_versions_before(elements);
      auto properties_to_move = elements - versions_to_move;

      // Move elements from to to from
      memcpy((char*) (to.start + to.edges_and_versions), (char*) from.start, elements * sizeof(dst_t));

      // Move elements in from backwards
      memmove((char*) from.start, (char*) (from.start + elements), (from.edges_and_versions - elements) * sizeof(dst_t));

      // Move Properties
      // Make place for properties
      memmove(to.properties_start() - properties_to_move * property_size, to.properties_start(), to.properties * property_size);

      // Move properties
      memcpy( to.end - properties_to_move * property_size , from.properties_start(), properties_to_move * property_size);

      from.edges_and_versions -= elements;
      from.properties -= properties_to_move;
      to.edges_and_versions += elements;
      to.properties += properties_to_move;
    }

    size_t count_edges(version_t version) {
      auto count = 0;
      for (auto i = start; i < start + edges_and_versions ; i++) {
        if (!is_versioned(*i)) {
          count++;
        } else {
          EdgeVersionRecord vr(make_unversioned(*i), i + 1, nullptr, false, property_size);
          if (vr.exists_in_version(version)) {
            count++;
          }
          i++; // Do not count the version record.
        }
      }
      return count;
    }

    dst_t* get_single_block_pointer() {
      return start;
    }

    size_t get_edges_and_versions() {
      return edges_and_versions;
    }

    size_t get_property_count() {
      return properties;
    }

    size_t get_block_capacity() {
      return capacity;
    }

    dst_t get_max_edge() {
      if (1 < edges_and_versions && is_versioned(start[edges_and_versions - 2])) {
        return make_unversioned(start[edges_and_versions - 2]);
      } else {
        return start[edges_and_versions - 1];
      }
    }

    dst_t get_min_edge() {
      return make_unversioned(*start);
    }

    void update_skip_list_header(VSkipListHeader *h) {
      h->size = edges_and_versions;
      h->properties = properties;
      h->max = get_max_edge();
    }

    char *properties_start() {
      return end - properties * property_size;
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
      for (auto i = start; i < start + edges_and_versions; i++) {
        auto e = make_unversioned(*i);
        cout << " " << e;
        if (is_versioned(*i)) {
          i++;  // Jump over version
        }

      }
      cout << endl;
      cout << "Logical Edges: " << endl;
      for (auto i = start; i < start + edges_and_versions; i++) {
        auto e = make_unversioned(*i);
        cout << " " << physical_to_logical(e);
        if (is_versioned(*i)) {
          i++;  // Jump over version
        }

      }
      cout << endl;
      cout << endl;
      cout << "Properties: " << endl;
      for (dst_t* i = (dst_t*) properties_start(); i < (dst_t*) properties_start() + properties; i++) {
        cout << " " << *i;
      }
      cout << endl;
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
    static dst_t* find_upper_bound(dst_t *start, dst_t *end, dst_t value) {
      auto l = 0;
      auto r = end - start;
      while ((r - l) > 16) {  // Incorrect if not ended before r-l > 4 because there could be an endless loop.
        auto m = l + (r - l) / 2;

        auto v = 0 < m && is_versioned(start[m - 1]) ? make_unversioned(start[m - 1]) : make_unversioned(start[m]);
        if (value > v) {
          l = m + 1;
        } else if (v == value) {
          if (0 < m && is_versioned(start[m - 1])) {
            return start + m - 1;
          } else {
            return start + m;
          }
        } else {
          r = m;
        }
      }
      dst_t *ptr = start + l;
      if (ptr != start && is_versioned(*(ptr - 1))) { // Do not start on a version record.
        ptr -= 1;
      }
      for (; ptr < end; ptr++) {
        auto v = *ptr;
        if (value <= make_unversioned(v)) {
          return ptr;
        }
        if (is_versioned(v)) {
          ptr++;  // Skip inline version record.
        }
      }
      return end;
    }

    /**
    * End of the memory region.
    */
    char* end;

private:
    size_t capacity;



    /**
     * The number of version records and edges.
     */

    size_t edges_and_versions;
    /**
     * The number of properties.
     * Equals the number of edges.
     */
    size_t properties;

    /**
     * The size in bytes of each property.
     */
    size_t property_size;

    size_t size() {
      return (end - (char*) start);
    };

    void insert_properties_by_shift(char *properties, size_t offset) {
      if (offset == 0) {
        memcpy(properties_start() - property_size, properties, property_size);
      } else {
        memmove(properties_start() - property_size, properties_start(), offset * property_size);
        memcpy(properties_start() + (offset - 1) * property_size, properties, property_size);
      }
    }

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
