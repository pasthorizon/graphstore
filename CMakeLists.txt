cmake_minimum_required(VERSION 3.10)
project(sortledton VERSION 1.0 DESCRIPTION "A Universal, Transactional Graph Data Structure")

set(CMAKE_CXX_STANDARD 17)

add_compile_options(-Wall -Wextra -Wno-unused-parameter -finline-functions)

# Compile with 32 bit identifiers. Not used.
#set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} -DBITS32)
set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} -DBITS64)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -march=native -mtune=native -shared-libgcc")

find_package(TBB REQUIRED COMPONENTS tbbmalloc tbbmalloc_proxy)
find_package (Threads)
find_package(OpenMP)

include_directories(.)
include_directories(data-structures)

set(SRC
        data-structure/TopologyInterface.cpp data-structure/TopologyInterface.h
        internal-driver/data-src/EdgeList.cpp internal-driver/data-src/EdgeList.h
        internal-driver/data-src/SortedCSRDataSource.cpp internal-driver/data-src/SortedCSRDataSource.h
        internal-driver/data-src/DataSource.cpp internal-driver/data-src/DataSource.h
        utils/NotImplemented.cpp utils/NotImplemented.h
        internal-driver/Driver.cpp internal-driver/Driver.h
        internal-driver/Configuration.cpp internal-driver/Configuration.h
        utils/utils.cpp utils/utils.h
        internal-driver/data-src/IOException.cpp internal-driver/data-src/IOException.h
        internal-driver/Reporter.cpp internal-driver/Reporter.h
        internal-driver/BFSSourceSelector.cpp internal-driver/BFSSourceSelector.h
        algorithms/Algorithms.cpp algorithms/Algorithms.h
        algorithms/PageRank.cpp algorithms/PageRank.h
        data-structure/data_types.cpp
        data-structure/SizeVersionChainEntry.cpp data-structure/SizeVersionChainEntry.h
        data-structure/VersionedTopologyInterface.cpp data-structure/VersionedTopologyInterface.h
        data-structure/VersioningBlockedSkipListAdjacencyList.cpp data-structure/VersioningBlockedSkipListAdjacencyList.h
        data-structure/Precondition.cpp data-structure/Precondition.h
        data-structure/IllegalOperation.cpp data-structure/IllegalOperation.h
        data-structure/Transaction.cpp data-structure/Transaction.h
        data-structure/SnapshotTransaction.cpp data-structure/SnapshotTransaction.h
	data-structure/AccessPointers.cpp data-structure/AccessPointers.cpp
	data-structure/FreeList.h data-structure/FreeList.cpp
	data-structure/TransactionManager.cpp data-structure/TransactionManager.h
        data-structure/VertexExistsPrecondition.cpp data-structure/VertexExistsPrecondition.h
        data-structure/EdgeDoesNotExistsPrecondition.cpp data-structure/EdgeDoesNotExistsPrecondition.h
	algorithms/GAPBSAlgorithms.cpp algorithms/GAPBSAlgorithms.h
        third-party/gapbs.h
        data-structure/VertexIndex.cpp
        data-structure/VertexIndex.h utils/pointerTagging.h
        data-structure/EdgeBlock.cpp data-structure/EdgeBlock.h
        data-structure/AdjacencySetTypes.h
        algorithms/WCC.cpp algorithms/WCC.h
        algorithms/CDLP.cpp algorithms/CDLP.h
        algorithms/SSSP.cpp algorithms/SSSP.h
        algorithms/LCC.cpp algorithms/LCC.h
        internal-driver/GraphalyticsProperties.cpp internal-driver/GraphalyticsProperties.h
        internal-driver/Experiments.cpp internal-driver/Experiments.h
        data-structure/VersionedBlockedEdgeIterator.cpp data-structure/VersionedBlockedEdgeIterator.h
        data-structure/memory_allocation/BlockProvider.cpp data-structure/memory_allocation/BlockProvider.h
        data-structure/EdgeVersionRecord.cpp data-structure/EdgeVersionRecord.h
        data-structure/VersionedBlockedPropertyEdgeIterator.cpp data-structure/VersionedBlockedPropertyEdgeIterator.h
        third-party/RWSpinLock.cpp third-party/RWSpinLock.h)

# Library to use with the GFE Driver
add_library(sortledton STATIC ${SRC})
target_link_libraries(sortledton OpenMP::OpenMP_CXX)
target_link_libraries (sortledton Threads::Threads)
target_link_libraries(sortledton tbb)
set_target_properties(sortledton PROPERTIES VERSION ${PROJECT_VERSION})
set_target_properties(sortledton PROPERTIES SOVERSION 1)

# Binary used for internal driver
add_executable(sortledton_bin internal-driver/main.cpp ${SRC})
target_link_libraries (sortledton_bin Threads::Threads)
target_link_libraries(sortledton_bin tbb)
target_link_libraries(sortledton_bin OpenMP::OpenMP_CXX)

# Dataset converter for internally binary input format.
set(DATASET_CONVERTER_SRC
        internal-driver/data-src/dataset-converter/DatasetConverter.cpp
        internal-driver/data-src/dataset-converter/Options.cpp
        internal-driver/data-src/SortedCSRDataSource.cpp
        internal-driver/data-src/DataSource.cpp
        utils/utils.cpp)
add_executable(dataset_converter internal-driver/data-src/dataset-converter/main.cpp ${DATASET_CONVERTER_SRC})


