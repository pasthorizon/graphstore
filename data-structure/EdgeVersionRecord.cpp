//
// Created by per on 30.05.21.
//

#include <cassert>
#include <forward_list>
#include <cstring>
#include "EdgeVersionRecord.h"
#include <utils/pointerTagging.h>
#include <utils/NotImplemented.h>


struct VersionChainRecord {
    VersionChainRecord(version_t v, weight_t w, EdgeOperation operation) : v(v), w(w), operation(operation) {};
    version_t v;
    weight_t w;
    EdgeOperation operation;
};

forward_list<VersionChainRecord> *get_chain(version_t v) {
  assert(v & MORE_VERSION_MASK);
  return (forward_list<VersionChainRecord> *) get_pointer(v);
}

forward_list<VersionChainRecord>::iterator
get_version_from_chain(forward_list<VersionChainRecord> &chain, version_t version) {
  auto i = chain.begin();
  while (i->v > version) {
    i++;
    assert(i != chain.end());  // This should not happen as the last entry in a chain has v == FIRST_VERSION.
  }
  return i;
}

EdgeVersionRecord::EdgeVersionRecord(dst_t e, version_t *v, char *w, bool has_weight, size_t weight_size)
  : e(e), v(v), w(w), has_weight(has_weight), weight_size(weight_size) {
  if (*v & MORE_VERSION_MASK) {
    state = MULTIPLE_VERSIONS;
  } else if (*v == FIRST_VERSION) {
    state = SINGLE_VERSION;
  } else {
    state = TWO_VERSIONS;
  }

}

weight_t EdgeVersionRecord::get_weight(version_t version) const {
  assert(has_weight);
  switch (state) {
    case SINGLE_VERSION:
    case TWO_VERSIONS: {
      return *(weight_t*) w;
    }
    case MULTIPLE_VERSIONS: {
      auto chain = get_chain(*v);
      auto i = get_version_from_chain(*chain, version);
      return i->w;
    }
    default: throw NotImplemented();
  }
}

bool EdgeVersionRecord::exists_in_version(version_t version) const {
  switch (state) {
    case SINGLE_VERSION: {
      return true;
    }
    case TWO_VERSIONS: {
      if (version >= timestamp(*v)) {
        return !is_deletion(*v);
      } else {
        return is_deletion(*v);
      }
    }
    case MULTIPLE_VERSIONS: {
      auto chain = get_chain(*v);
      auto i = get_version_from_chain(*chain, version);
      return i->operation != DELETION;
    }
    default:
      throw NotImplemented();
  }
}

void EdgeVersionRecord::gc(version_t min_version, const vector<version_t>& sorted_active_versions) {
  if (state == MULTIPLE_VERSIONS) {
    // Get the chain and the element read by min_version.
    auto  chain = get_chain(*v);

    // List used to store removed versions.
    forward_list<VersionChainRecord> to_drop;

    auto i = 0u; // Offset of the current active version.
    auto current = chain->begin(); // Will point to the version read by the current active version
    auto before = chain->begin(); // Will point to the version read by the active version before current.

    // Find the youngest version read by an active transaction.
    for (; i < sorted_active_versions.size(); i++) {
      auto version = sorted_active_versions[i];
      if (version == NO_TRANSACTION) {
        continue;
      }
      while (current->v > version) {
        current++;
        before++;
      }
      break;
    }
    // Current and before now points to the youngest read version in this chain.
    // We do not collect before to not complicate the process of building the list of sorted active transactions.

    i++; // We are not interested to the version older than the last v.
    for (; i < sorted_active_versions.size(); i++) {
      while (current->v > sorted_active_versions[i]) {
        current++;
      }

      // Remove versions between
      if (current != before) {
        to_drop.splice_after(to_drop.before_begin(), *chain, before, current);
      }
      before = current;
    }

    // Current is now the oldest version read by any transaction. Mark it as FIRST_VERSION.
    current->v = FIRST_VERSION;

    // Collect all versions older than read by the oldest transaction
    to_drop.splice_after(to_drop.before_begin(), *chain, current, chain->end());

    auto size = 0;
    auto iter = chain->begin();
    while (iter != chain->end() && size < 3) {
      size += 1;
      iter++;
    }

    // We try to inline short lists.
    bool inlined = false;
    switch (size) {
      case 1: {
        auto r = chain->begin();
        *v = r->v;
        if (r->operation == DELETION) {
          *v |= DELETION_MASK;
        }
        inlined = true;
        break;
      }
      case 2: {
        auto r1 = *chain->begin();
        auto r2 = *chain->begin()++;

        // We have only one weight and can inline
        if (r1.operation == DELETION || r2.operation == DELETION) {
          *v = r1.v;
          if (r1.operation == DELETION) {
            *v |= DELETION_MASK;
          }
          inlined = true;
        }
        break;
      }
      default: {
        // NOP cannot inline
      }
    }

    if (inlined) {
      state = TWO_VERSIONS;
      free(chain);
    }
  }
}

void EdgeVersionRecord::write(version_t version, EdgeOperation kind, char *weight) {
  assert(has_weight);
  assert_can_write(version, kind);

  switch (state) {
    case SINGLE_VERSION: {
      switch (kind) {
        case UPDATE: {
          create_version_chain();
          write(version, kind, weight);
          return;
        }
        case INSERTION: {
          *v = version;
          *w = copy_weight(weight);
          break;
        }
        case DELETION: {
          *v = version;
          *v |= DELETION_MASK;
          break;
        }
      }
      break;
    }
    case TWO_VERSIONS: {
      create_version_chain();
      write(version, kind, weight);
      return;
    }
    case MULTIPLE_VERSIONS: {
      auto chain = (forward_list<VersionChainRecord> *) get_pointer(*v);
      weight_t temp = weight != nullptr ? copy_weight(weight) : 0;
      if (kind != DELETION) {
        *w = temp;
      }
      chain->push_front(VersionChainRecord(version, temp, kind));
      break;
    }
  }
}

void EdgeVersionRecord::create_version_chain() {
  assert(has_weight);
  switch (state) {
    case SINGLE_VERSION: {
      auto chain = new forward_list<VersionChainRecord>();
      chain->push_front(VersionChainRecord(FIRST_VERSION, *w, INSERTION));
      *v = (version_t) chain;
      *v |= MORE_VERSION_MASK;
      state = MULTIPLE_VERSIONS;
      break;
    }
    case TWO_VERSIONS: {
      auto chain = new forward_list<VersionChainRecord>();
      auto first_operation = is_deletion(*v) ? INSERTION : DELETION;
      auto second_operation = is_deletion(*v) ? DELETION : INSERTION;

      chain->push_front(VersionChainRecord(FIRST_VERSION, *w, first_operation));
      chain->push_front(VersionChainRecord(timestamp(*v), *w, second_operation));
      *v = (version_t) chain;
      *v |= MORE_VERSION_MASK;
      state = MULTIPLE_VERSIONS;
      break;
    }
    case MULTIPLE_VERSIONS: {
      return;
    }

  }

}

weight_t EdgeVersionRecord::copy_weight(char *weight) {
  weight_t temp = 0;
  memcpy((char *) &temp, weight, weight_size);
  return temp;
}

void EdgeVersionRecord::assert_can_write(version_t version, EdgeOperation operation) {
#ifdef DEBUG
  version_t latest_timestamp = NO_TRANSACTION;
  EdgeOperation latest_operation;
  switch (state) {
    case SINGLE_VERSION: {
      return; // We can always write
    }
    case TWO_VERSIONS: {
      latest_timestamp = timestamp(*v);
      latest_operation = is_deletion(*v) ? DELETION : UPDATE;
      break;
    }
    case MULTIPLE_VERSIONS: {
      auto chain = get_chain(*v);
      auto r = *(chain->begin());
      latest_timestamp = r.v;
      latest_operation = r.operation;
      break;
    }
    default: {
      throw NotImplemented();
    }
  }

  assert (latest_timestamp <= version);
  switch (operation) {
    case DELETION: {
      assert(latest_operation != DELETION);
      break;
    }
    case INSERTION: {
      assert(latest_operation != INSERTION);
      break;
    }
    case UPDATE: {
      assert(latest_operation != DELETION);
    }

  }
#endif
}

void EdgeVersionRecord::assert_version_list(version_t min_version) {
  assert(state == MULTIPLE_VERSIONS);
  auto chain = get_chain(*v);

  auto i = chain->begin();
  version_t last_timestamp = NO_TRANSACTION;
  while (i != chain->end()) {
    assert(i->v < last_timestamp);
    last_timestamp = i->v;
    i++;
  }
  assert(last_timestamp == FIRST_VERSION);
}

vector<version_t> EdgeVersionRecord::get_versions() {
  vector<version_t> versions;
  switch (state) {
    case MULTIPLE_VERSIONS: {
      auto chain = get_chain(*v);
      for (auto& v : *chain) {
        versions.push_back(v.v);
      }
      break;
    }
    case SINGLE_VERSION: {
      break;
    }
    case TWO_VERSIONS: {
      versions.push_back(timestamp(*v));
      break;
    }
    default: throw NotImplemented();
  }
  return versions;
}

