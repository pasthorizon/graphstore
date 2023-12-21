#include "FreeList.h"
#include<iostream>
mutex FreeList::change;

map<version_t, version_list> FreeList::free_list;

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
            cout<<"inserting "<<version<<endl;
            free_list.insert(make_pair(version, version_list()));
            free_list[version].lock = new RWSpinLock();
    }
    // change.unlock();
    // free_list[version].lock->lock();
    // change.lock();
        free_list[version].free_list.splice_after(free_list[version].free_list.before_begin(), other);
    change.unlock();
    // free_list[version].lock->unlock();
}

void FreeList::eraseVersion(version_t version){
    change.lock();
        for(auto x: free_list)
            cout<<x.first<<" j" <<endl;
        if(free_list[version].lock)
            delete free_list[version].lock;
        free_list.erase(version);
    change.unlock();
}

bool FreeList::get_next_block(version_t version, free_block& ans){
    // if(version)
    if(free_list[version].lock==nullptr) return false;

    free_list[version].lock->lock();
    // change.lock();
    bool ret;
    if(free_list[version].free_list.empty()){
        ret = false;
    }    
        
    else{
        ans = free_list[version].free_list.front();
        free_list[version].free_list.pop_front();
        ret = true;
    }
    free_list[version].lock->unlock();
    // change.unlock();
    return ret;
}