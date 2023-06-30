//
// Created by per on 07.04.21.
//

#ifndef LIVE_GRAPH_TWO_HUGEPAGEBACKEDPOOL_H
#define LIVE_GRAPH_TWO_HUGEPAGEBACKEDPOOL_H


#include <cstdlib>
#include <vector>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

using namespace std;

class BlockProvider {
public:
    explicit BlockProvider(size_t max_block_size);
    ~BlockProvider();

    BlockProvider(BlockProvider& other) = delete;
    BlockProvider& operator=(const BlockProvider&) = delete;

    void* get_block(size_t block_size);
    void free_block(void* block, size_t block_size);

private:
    const size_t max_block_size;

    mutex lock;
    unordered_map<size_t, vector<void*>> free_lists;
    vector<void*> pages;

#ifdef DEBUG
    unordered_map<size_t, unordered_set<void*>> used_list;
#endif

    void* get_block_unsafe(size_t block_size, bool migrating_size = false);
    void add_page();

};


#endif //LIVE_GRAPH_TWO_HUGEPAGEBACKEDPOOL_H
