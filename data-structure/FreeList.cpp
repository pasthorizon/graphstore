#include "FreeList.h"
#include<iostream>
mutex FreeList::change;

map<version_t, forward_list<free_block>> FreeList::free_list;

void FreeList::add_node(void* node, version_t version, int type){ 
    if(version > last_version){
        
        if(!local_list.empty())
            FreeList::merge_lists(local_list, last_version);
        curr_block_elements=0;
        last_version = version;
    }

    if(local_list.empty() || curr_block_elements==per_block){
        local_list.push_front(free_block(per_block));
        curr_block_elements = 0;
    }
    
    (local_list.front()).block[curr_block_elements] = node;
    (local_list.front()).type[curr_block_elements++] = type;
    (local_list.front()).num_elements++;

}

void FreeList::merge_lists(forward_list<free_block>& other, version_t version){
    change.lock();
    if(free_list.find(version)==free_list.end()){
        free_list.insert(make_pair(version,forward_list<free_block>()));
    }
    free_list[version].splice_after(free_list[version].before_begin(), other);
    change.unlock();
}


bool FreeList::get_next_block(version_t version, free_block& ans){
    change.lock();  bool ret;
    if(free_list[version].empty()){
        ret = false;
    }    
        
    else{
        ans = free_list[version].front();
        free_list[version].pop_front();

        ret = true;
    }
    change.unlock();
    return ret;
}