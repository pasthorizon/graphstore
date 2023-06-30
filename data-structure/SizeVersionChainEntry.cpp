//
// Created by per on 13.01.21.
//

#include "SizeVersionChainEntry.h"

#include <cassert>

SizeVersionChainEntry::SizeVersionChainEntry(version_t version, uint32_t current_size)
                                             : version(version), current_size(current_size) {

}
