#!/bin/bash
# Exp0: motivating experiment; please run in the prototype build folder
SCRIPTS_DIR=$(realpath $(dirname $0))

source ${SCRIPTS_DIR}/../common.sh
cd ${SCRIPTS_DIR}/../../build/

# m: 0 zone write, 1 zone append
# c: number of concurrent writes
# z: number of open zones
# s: request size
# number of zones
for repeat in 0 1 2 3 4; do
  for s in 1 2 4 8 16; do
    mkdir -p motivating_results/${s}
    for m in 0; do
      for c in 1; do
        echo "sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z 1 > motivating_results/${s}/${m}_${c}_${repeat}.result"
        file=motivating_results/${s}/${m}_${c}_${repeat}.result
        if [[ ! -f $file ]]; then
          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z 1 # > $file
        fi
        exit
      done
    done
    for m in 1; do
      for c in 1 2 3 4; do
        file=motivating_results/${s}/${m}_${c}_${repeat}.result
        echo "sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z 1 > $file"
        if [[ ! -f $file ]]; then
          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z 1 > $file 
        fi
      done
    done
  done
done

for repeat in 0 1 2 3 4; do
  for s in 1 2 4 8 16; do
    mkdir -p motivating_results/concurrent_reqs/${s}
    for m in 0; do
      for c in 1; do
        for z in 1 2 3 4 5 6 7 8; do
          file=motivating_results/concurrent_reqs/${s}/${m}_${c}_${z}_${repeat}.result
          echo "sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z ${z} > $file"
          if [[ ! -f $file || $(ls -s $file | awk '{print $1;}') -eq 0 ]]; then
            sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z ${z} > $file
          fi
       done 
      done
    done
    for m in 1; do
      for c in 4; do
        for z in 1 2 3 4 5 6 7 8; do
          file=motivating_results/concurrent_reqs/${s}/${m}_${c}_${z}_${repeat}.result
          echo "sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z ${z} > $file"
          if [[ ! -f $file || $(ls -s $file | awk '{print $1;}') -eq 0 ]]; then
            sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z ${z} > $file
          fi
        done
      done
    done
  done
done
