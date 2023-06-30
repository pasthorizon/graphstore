//
// Created by per on 13.01.21.
//

#include "data_types.h"

//bool is_versioned(dst_t e) {
//  return VERSION_MASK & e;
//}
//
//dst_t make_versioned(dst_t e) {
//  return e | VERSION_MASK;
//}
//
//dst_t make_unversioned(dst_t e) {
//  return e & ~VERSION_MASK;
//}

bool more_versions_existing(version_t v) {
  return MORE_VERSION_MASK & v;
}

bool is_deletion(version_t v) {
  return DELETION_MASK & v;
}

version_t timestamp(version_t v) {
  return (v & ~MORE_VERSION_MASK) & ~DELETION_MASK;
}
