#include "FreeList.h"
#include <mutex>

mutex change;

void FreeList::add_node(void* node, version_t version){ 
    if(free_list.find(version)==free_list.end()){
        curr_block_elements[version] = 0;
        free_list.insert(make_pair(version,forward_list<free_block>()));
    }
    if(free_list[version].empty() || curr_block_elements[version]==per_block){
        free_list[version].push_front(free_block(per_block));
        curr_block_elements[version] = 0;
    }
    
    (free_list[version].front()).block[curr_block_elements[version]++] = node;
}

void FreeList::add_block(free_block other, version_t version){
    change.lock();
    if(free_list.find(version)==free_list.end()){
        curr_block_elements[version] = 0;
        free_list.insert(make_pair(version,forward_list<free_block>()));
    }
    free_list[version].push_front(other);
    change.unlock();
}


bool FreeList::get_next_block(version_t version, free_block& ans){
    change.lock();
    if(free_list[version].empty())
    return false;
    else{
        ans = free_list[version].front();
        free_list[version].pop_front();
        change.unlock();
        return true;
    }
}