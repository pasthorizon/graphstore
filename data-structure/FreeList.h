
#ifndef LIVE_GRAPH_TWO_FREELIST_H
#define LIVE_GRAPH_TWO_FREELIST_H

#include"AdjacencySetTypes.h"
#include"data_types.h"
#include<forward_list>
#include<mutex>
#include<map>
#include<iostream>
using namespace std;

typedef struct free_block{
    void **block;
    int type[512];
    int num_elements;
    free_block(int per_block){
        // cout<<"free_block constructor called\n"<<endl;
        num_elements=0;
        block = (void**) malloc(sizeof(void*)*per_block);
    }
    free_block(const free_block& other){
        // cout<<"copy contructor called"<<endl;
        num_elements = other.num_elements;
        block = other.block;
        for(int i=0;i<512;i++)
            type[i] = other.type[i];
    }

    free_block& operator=(const free_block& other){
        // cout<<"assignment operator overloading working\n";
        this->block = other.block;
        this->num_elements = other.num_elements;
        // cout<<"exitingthe assignment operator constructor"<<endl;
        for(int i=0;i<512;i++)
            type[i] = other.type[i];
        return *this;
    }

}free_block;

class FreeList{
    public:
        
        void add_node(void* node, version_t version, int type);

        static bool get_next_block(version_t version, free_block& ans);

        FreeList(int per_block):per_block(per_block){
            last_version = 0;
            curr_block_elements = 0;
            // cout<<"\nFreeList constructor called and "<<last_version<<endl;
        }

        static map<version_t, forward_list<free_block>> free_list;
        static void merge_lists(forward_list<free_block>& other, version_t version);

    private:
        int per_block;
        version_t last_version;
        int curr_block_elements;
        static mutex change; 
        forward_list<free_block> local_list;
};


#endif