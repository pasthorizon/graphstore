//
// Created by per on 31.08.20.
//

#ifndef LIVE_GRAPH_TWO_NOTIMPLEMENTED_H
#define LIVE_GRAPH_TWO_NOTIMPLEMENTED_H

#include <exception>
#include <string>

using namespace std;

class NotImplemented : exception {
public:
  NotImplemented(): NotImplemented("") { } ;
  explicit NotImplemented(string what) : exception(), w(what) {};
  const char* what() const noexcept override { return w.c_str(); };
private:
    string w;
};


#endif //LIVE_GRAPH_TWO_NOTIMPLEMENTED_H
