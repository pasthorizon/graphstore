#include "AccessPointers.h"
#include "TopologyInterface.h"
#include<iostream>
#include <set>
AllInlineAccessPointers::AllInlineAccessPointers(){
    for(int i=0;i<MAX_EPOCHS;i++)
    {
        versions[i].store(0);
        pointers[i] = 0;
    }
}

AllInlineAccessPointers::AllInlineAccessPointers(const AllInlineAccessPointers& other){
    for(int i=0;i<MAX_EPOCHS;i++)
    {
        versions[i].store(0);
        pointers[i] = 0;
    }
    versions[0].store(other.get_latest_version());
    pointers[0] = other.get_latest_pointer();
}

version_t AllInlineAccessPointers::get_latest_version() const{
    return versions[0].load();
}


void *AllInlineAccessPointers::get_latest_pointer() const{
    return pointers[0];
}

void *AllInlineAccessPointers::get_pointer(version_t version, bool debug) const{
    // cout<<"not happening here"<<endl<<endl;
    int ans=0; int diff=1e9;
    for(int i=0;i<MAX_EPOCHS;i++){
        if(versions[i].load()<=version && diff > abs((long long)versions[i].load()-(long long)version))
            ans = i, diff = abs((long long)versions[i].load()-(long long)version);
        version_t curr = versions[i].load();
        if(debug)
            cout<<curr<<" ";
    }
    if(debug)
    cout<<endl;
    

    return pointers[ans];
}


void AllInlineAccessPointers::add_new_pointer(void *pointer, version_t version, version_t minActive){
    int toReplace=0;

    if(versions[0].load()!=version){
        for(int i=1;i<MAX_EPOCHS;i++)
        {
            if(versions[i].load()<minActive){
                toReplace = i;
                break;
            }
        }

        pointers[toReplace] = pointers[0];
        versions[toReplace].store(versions[0].load());

        pointers[0] = pointer;
        versions[0].store(version);
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
    int ans=0;
    for(int i=0;i<MAX_EPOCHS;i++)
    if(versions[i].load()>=versions[ans].load() && versions[i].load()<=version)
        ans = i;
    return {sizes[ans], versions[ans].load()};
}

void  AllInlineAccessPointersWithSize::add_new_pointer(void *pointer, version_t version, version_t minActive){
    int toReplace=0;

    if(versions[0].load()!=version){
        for(int i=1;i<MAX_EPOCHS;i++)
        {
            if(versions[i].load()<minActive){
                toReplace = i;
                break;
            }
        }

        pointers[toReplace] = pointers[0];
        versions[toReplace].store(versions[0].load());
        sizes[toReplace] = sizes[0];

        pointers[0] = pointer;
        versions[0].store(version);
    }
    else pointers[0] = pointer;
}




