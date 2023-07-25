
#ifndef LIVE_GRAPH_TWO_FREELIST_H
#define LIVE_GRAPH_TWO_FREELIST_H

#include"AdjacencySetTypes.h"
#include"data_types.h"
#include<forward_list>
#include<map>
using namespace std;

typedef struct free_block{
    void **block;
    free_block(int per_block){
        block = (void**) malloc(sizeof(void*)*per_block);
    }
    free_block(const free_block& other){
        block = other.block;
    }

    free_block& operator=(const free_block& other){
        this->block = other.block;
    }

}free_block;

class FreeList{
    public:
        void add_block(free_block other, version_t version);
        void add_node(void* node, version_t version);

        bool get_next_block(version_t version, free_block& ans);

        FreeList(int per_block):per_block(per_block){

        }

    private:
        int per_block;
        map<version_t,int> curr_block_elements;
        map<version_t, forward_list<free_block>> free_list;

};


#endif