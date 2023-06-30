#!/usr/bin/env bash

src_dir=$1
graph_name=$2

dc="/home/fuchs/graph-two/cmake-build-release-scyper15/dataset_converter"
bi="python3 /home/fuchs/graph-two/data-src/dataset-converter/binarize_graphalytics_model_solution.py"

ALL_INSERT="--insert_percentage 1.0 --delete_percentage 0.0"

cd $src_dir || exit 1

rm -f "${src_dir}/insertions.edgeList"

$dc ${ALL_INSERT} --weighted --make-undirected ${graph_name}.e ./

for a in "PR" "LCC"
do
  b=$(echo $a | awk '{print tolower($0)}')
  $bi "./${graph_name}-${a}" "./${b}.gold_standard" double
done


for a in "CDLP" "WCC"
do
  b=$(echo $a | awk '{print tolower($0)}')
  $bi "./${graph_name}-${a}" "./${b}.gold_standard" ulong
done

for a in "BFS"
do
  b=$(echo $a | awk '{print tolower($0)}')
  echo $bi "./${graph_name}-${a}" "./${b}.gold_standard" uint
done
