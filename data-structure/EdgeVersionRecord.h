//
// Created by per on 30.05.21.
//

#ifndef LIVE_GRAPH_TWO_EDGEVERSIONRECORD_H
#define LIVE_GRAPH_TWO_EDGEVERSIONRECORD_H


#include <data-structure/data_types.h>
#include <vector>
enum EdgeOperation{
    INSERTION,
    UPDATE,
    DELETION
};

/**
 * Represents a version of an Edge.
 */
class EdgeVersionRecord {
public:
    EdgeVersionRecord(dst_t e, version_t* v, char* w, bool has_weight, size_t weight_size);

    bool exists_in_version(version_t version) const;
    weight_t get_weight(version_t version) const;

    void write(version_t version, EdgeOperation kind, char* weight);

    void gc(version_t min_version, const vector<version_t> &sorted_active_versions);

    vector<version_t> get_versions();

    void assert_version_list(version_t min_version);

private:
    enum VersionState {
        SINGLE_VERSION,
        TWO_VERSIONS,
        MULTIPLE_VERSIONS
    };

    VersionState state;
    dst_t e = numeric_limits<dst_t>::max();
    version_t* v = nullptr;
    char* w = nullptr;
    bool has_weight = false;
    size_t weight_size = 0;

    void create_version_chain();
    weight_t copy_weight(char *weight);

    void assert_can_write(version_t version, EdgeOperation operation);
};


#endif //LIVE_GRAPH_TWO_EDGEVERSIONRECORD_H
