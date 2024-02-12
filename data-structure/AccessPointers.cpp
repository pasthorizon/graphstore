#include "AccessPointers.h"
#include "TopologyInterface.h"
#include "AdjacencySetTypes.h"
#include<iostream>
#include <set>
 

AllInlineAccessPointers::AllInlineAccessPointers(){
    for(int i=0;i<MAX_EPOCHS;i++)
    {
        versions[i] = 0;
        pointers[i] = 0;
    }
}

AllInlineAccessPointers::AllInlineAccessPointers(const AllInlineAccessPointers& other){
    for(int i=0;i<MAX_EPOCHS;i++)
    {
        versions[i] = 0;
        pointers[i] = 0;
    }
    versions[0] = other.get_latest_version();
    pointers[0] = other.get_latest_pointer();
}

version_t AllInlineAccessPointers::get_latest_version() const{
    return versions[0];
}


void *AllInlineAccessPointers::get_latest_pointer() const{
    return pointers[0];
}

void *AllInlineAccessPointers::get_pointer(version_t version, bool debug) const{
    // cout<<"not happening here"<<endl<<endl;
    int ans=0; int diff=1e9;
    version_t latest_version = versions[0];
    if(latest_version==version) return pointers[0];

    if(latest_version<=version && diff > abs((long long)latest_version-(long long)version))
        ans = 0, diff = abs((long long)latest_version-(long long)version);

    for(int i=1;i<MAX_EPOCHS;i++){
        version_t this_version = versions[i];
        if(this_version<=version && diff > abs((long long)this_version-(long long)version))
            ans = i, diff = abs((long long)this_version-(long long)version);
        
    }

    return pointers[ans];
}


void AllInlineAccessPointers::add_new_pointer(void *pointer, version_t version, version_t minActive, int type){
    int toReplace=0;
    version_t replaced_version = versions[0];
    if(replaced_version!=version){
        for(int i=1;i<MAX_EPOCHS;i++)
        {
            if(versions[i]<minActive){
                toReplace = i;
                break;
            }
        }

        pointers[toReplace] = pointers[0];
        versions[toReplace] = replaced_version;

        pointers[0] = pointer;
        versions[0] = version;

    }
    else pointers[0] = pointer;
}




AllInlineAccessPointersWithSize::AllInlineAccessPointersWithSize():AllInlineAccessPointers(){
    for(int i=0;i<MAX_EPOCHS;i++)
        sizes[i]=0;
}



AllInlineAccessPointersWithSize::AllInlineAccessPointersWithSize(const AllInlineAccessPointersWithSize& other):AllInlineAccessPointers(other){
    for(int i=0;i<MAX_EPOCHS;i++)
        sizes[i]=0;
}

tuple<uint64_t,version_t> AllInlineAccessPointersWithSize::get_size(version_t version){
    int ans=0; long long diff = 1e18;
    version_t latest_version = versions[0];
    if(latest_version==version)
        return {sizes[0], latest_version};
    version_t ans_version = INT64_MAX;

    if(latest_version<=version && diff > abs((long long)latest_version-(long long)version))
            ans = 0, diff = abs((long long)latest_version-(long long)version), ans_version = latest_version;


    for(int i=1;i<MAX_EPOCHS;i++){
        version_t this_version = versions[i];
        if(this_version<=version && diff > abs((long long)this_version-(long long)version))
            ans = i, diff = abs((long long)this_version-(long long)version), ans_version = this_version;
    }
    
    return {sizes[ans], ans_version};
}

void  AllInlineAccessPointersWithSize::add_new_pointer(void *pointer, version_t version, version_t minActive, int type){
    int toReplace=0;
    version_t replaced_version = versions[0];
    if(replaced_version!=version){
        for(int i=1;i<MAX_EPOCHS;i++)
        {
            if(versions[i] < minActive){
                toReplace = i;
                break;
            }
        }

        pointers[toReplace] = pointers[0];
        versions[toReplace] = replaced_version;
        sizes[toReplace] = sizes[0];

        pointers[0] = pointer;
        versions[0] = version;
    }
    else pointers[0] = pointer;
}




