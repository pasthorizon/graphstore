//
// Created by per on 09.10.20.
//

#ifndef LIVE_GRAPH_TWO_BFSSOURCESELECTOR_H
#define LIVE_GRAPH_TWO_BFSSOURCESELECTOR_H


#include "data-structure/TopologyInterface.h"
#include "internal-driver/Driver.h"
#include "Configuration.h"

class BFSSourceSelector {
public:
    BFSSourceSelector(Driver& driver, Dataset dataset, TopologyInterface& ds) : driver(driver), dataset(dataset), ds(ds) {};

    vertex_id_t get_source();
private:
    Driver& driver;
    Dataset dataset;
    TopologyInterface& ds;

    const static string SOURCE_FOLDER;
    constexpr static float TARGET_PERCENTAGE = 0.75;

    /**
     * Finds a source by running bfs from different sources until it finds one that traverses at least TARGET_PERCENTAGE
     * of all vertices.
     *
     * @return a source for which bfs encounters more than TARGER_PERCENTAGE of all vertices.
     */
    vertex_id_t find_source();

};


#endif //LIVE_GRAPH_TWO_BFSSOURCESELECTOR_H
