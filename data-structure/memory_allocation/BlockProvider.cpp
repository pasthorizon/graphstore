//
// Created by per on 16.04.21.
//

#include "BlockProvider.h"
#include <sys/mman.h>
#include <cassert>

#define MB_TO_RESERVE 1
#define LENGTH (MB_TO_RESERVE*1024*1024*1024)

#define MAP_HUGE_1GB (30 << MAP_HUGE_SHIFT)

BlockProvider::BlockProvider(size_t max_block_size) : max_block_size(max_block_size) {
  scoped_lock<mutex> l(lock);
  free_lists.insert(make_pair(max_block_size, vector<void*> {}));
  add_page();
}

BlockProvider::~BlockProvider() {
#ifdef DEBUG
//  for (const auto& e : used_list) {
//    assert(e.second.empty());
//  }
// TODO somewhere I'm missing a free or add a few too many max blocks here
#endif
  for (auto p : pages) {
    if (munmap(p, LENGTH)) {
      perror("munmap");
      exit(1);
    }
  }
}

void *BlockProvider::get_block(size_t size) {
  scoped_lock<mutex> l(lock);
  return get_block_unsafe(size);
}

void BlockProvider::free_block(void* block, size_t size) {
  scoped_lock<mutex> l(lock);
  free_lists[size].push_back(block);

#ifdef DEBUG
  used_list[size].erase(block);
#endif
}

void BlockProvider::add_page() {
  char* r = (char*) mmap(nullptr, LENGTH, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_HUGE_1GB, -1, 0);
  if (r == MAP_FAILED) {
    perror("mmap");
    exit(1);
  }

  pages.push_back(r);

  for (auto i = r; i < r + LENGTH - max_block_size; i += max_block_size) {
    free_lists[max_block_size].push_back(i);
  }
}

void *BlockProvider::get_block_unsafe(size_t block_size, bool migrating_size) {
  auto e = free_lists.find(block_size);
  if (e == free_lists.end()) {
    free_lists.insert(make_pair(block_size, vector<void*> { }));
    return get_block_unsafe(block_size);
  } else {
    auto& fl = e->second;
    if (fl.empty()) {
      if (block_size < max_block_size) {
        auto max_block = (char*) get_block_unsafe(max_block_size, true);

        for (auto i = max_block; i < max_block + max_block_size - block_size; i += block_size) {
          e->second.push_back(i);
        }
        return get_block_unsafe(block_size);
      } else {
        assert(block_size == max_block_size);
        add_page();
        return get_block_unsafe(block_size);
      }
    } else {
      auto r = fl.back();
      fl.pop_back();

      if (!migrating_size) {
#ifdef DEBUG
        used_list[block_size].insert(r);
#endif
      }
      return r;
    }
  }
}
