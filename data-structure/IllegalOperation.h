//
// Created by per on 23.12.20.
//

#ifndef LIVE_GRAPH_TWO_ILLEGALOPERATION_H
#define LIVE_GRAPH_TWO_ILLEGALOPERATION_H

#include <exception>
#include <string>

using namespace std;

class IllegalOperation : exception {
public:
    IllegalOperation();
    explicit IllegalOperation(string what);
    string what;
};


#endif //LIVE_GRAPH_TWO_ILLEGALOPERATION_H
