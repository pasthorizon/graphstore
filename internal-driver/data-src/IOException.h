//
// Created by per on 01.09.20.
//

#ifndef LIVE_GRAPH_TWO_IOEXCEPTION_H
#define LIVE_GRAPH_TWO_IOEXCEPTION_H

#include <exception>
#include <string>

using namespace std;

class IOException : exception {
public:
  explicit IOException(string what) : w(what) {}
  const char* what() const noexcept override { return w.c_str(); }
private:
    string w;
};


#endif //LIVE_GRAPH_TWO_IOEXCEPTION_H
