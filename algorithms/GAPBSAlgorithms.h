//
// Created by per on 10.02.21.
//

#ifndef LIVE_GRAPH_TWO_GAPBSALGORITHMS_H
#define LIVE_GRAPH_TWO_GAPBSALGORITHMS_H

#include <third-party/gapbs.h>

using namespace gapbs;

class GAPBSAlgorithms {
public:
  static pvector<int64_t> bfs(TopologyInterface& ti, uint64_t start_vertex, bool raw_neighbourhood, int alpha = 15, int beta = 18);
};


#endif //LIVE_GRAPH_TWO_GAPBSALGORITHMS_H
