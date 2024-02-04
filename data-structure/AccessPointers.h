
#ifndef LIVE_GRAPH_TWO_ACCESSPOINTERS_H
#define LIVE_GRAPH_TWO_ACCESSPOINTERS_H

#include "data_types.h"
#include <atomic>
class AccessPointers{

    public:
        virtual void add_new_pointer(void *pointer, version_t version, version_t minActive=0, int type = 0) = 0;
        virtual void *get_latest_pointer() const = 0;
        virtual version_t get_latest_version() const = 0;
        virtual void *get_pointer(version_t version, bool debug = false) const=0;
};


class AllInlineAccessPointers: public AccessPointers{
    public:
        AllInlineAccessPointers();
        AllInlineAccessPointers(const AllInlineAccessPointers& other);

        version_t versions[MAX_EPOCHS];
        void* pointers[MAX_EPOCHS];

        virtual void add_new_pointer(void *pointer, version_t version, version_t minActive=0, int type = 0);
        virtual void *get_latest_pointer() const;
        virtual version_t get_latest_version() const;
        virtual void *get_pointer(version_t version, bool debug=false) const;
};



class AllInlineAccessPointersWithSize: public AllInlineAccessPointers{
    public: 
        AllInlineAccessPointersWithSize();
        AllInlineAccessPointersWithSize(const AllInlineAccessPointersWithSize& other);
        virtual void add_new_pointer(void *pointer, version_t version, version_t minActive=0, int type = 0);
        tuple<uint64_t, version_t> get_size(version_t version);

        uint64_t sizes[MAX_EPOCHS];

};

#endif