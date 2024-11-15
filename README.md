A modified version of sortledton optimised for analytical queries

To build all executibles and static Sortledton library:
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j

To build the static Sortledton library:

mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j sortledton

