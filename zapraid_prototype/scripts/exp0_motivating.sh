#!/bin/bash
# Exp0: motivating experiment; please run in the prototype build folder
SCRIPTS_DIR=$(realpath $(dirname $0))

source ${SCRIPTS_DIR}/common.sh
cd ${SCRIPTS_DIR}/../build/

# m: 0 zone write, 1 zone append
# c: number of concurrent writes
# z: number of open zones
# s: request size
# number of zones

DIR="motivating_results"
for repeat in 0 1 2 3 4; do
  for s in 1 2 4; do
    mkdir -p $DIR/${s}
    for c in 4; do
      for m in 0 1; do 
        for z in 1 2 3 4 5 6 7 8; do 
          file=$DIR/${s}/${m}_${c}_${z}_${repeat}.result
          echo "output $file"
          if [[ ! -f $file || $(grep "p99" $file | wc -l) -eq 0 ]]; then
            sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z ${z} -y 0 -t 512 > $file
          fi
        done
      done
    done
  done
done
