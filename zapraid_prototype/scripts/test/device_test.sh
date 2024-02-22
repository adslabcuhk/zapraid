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

DIR="motivating_results"
DIR="zone_write_latency_hybrid"
DIR="zone_write"
DIR="zone_write_test_16k"
DIR="zone_write_test_new"

for dev in 4; do
for repeat in 0 1 2 3 4; do
  for s in 4; do
    mkdir -p $DIR/${s}
    for c in 1; do
      for m in 0; do 
#        for z in 1 2 3 4 5 6 7 8; do # 2 4 8 16; do
        for z in 3; do # 2 4 8 16; do
          file=$DIR/${s}/${m}_${c}_${z}_1_${repeat}.result
          file=$DIR/${s}/${m}_${c}_${z}_dev${dev}_${repeat}.result
          echo "output $file"
#          if [[ ! -f $file ]]; then
            sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z ${z} -y 0 -d ${dev} | tee $file
#          fi
        done
      done
    done
  done
  exit
done
done
#done

exit

for repeat in 0 1 2 3 4; do
  for s in 1 2 4 8; do
#for repeat in 0 1 2 3 4; do
#  for s in 1 2 4 8 16; do
    mkdir -p $DIR/${s}
#    for m in 0; do
#      for c in 1; do
#        echo "sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z 1 > $DIR/${s}/${m}_${c}_${repeat}.result"
#        file=$DIR/${s}/${m}_${c}_${repeat}.result
#        if [[ ! -f $file ]]; then
#          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z 1 > $file
#        fi
#      done
#    done
#    for m in 1; do
#      for c in 1 2 3 4; do
#        file=$DIR/${s}/${m}_${c}_${repeat}.result
#        echo "sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z 1 > $file"
#        if [[ ! -f $file ]]; then
#          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z 1 > $file 
#        fi
#      done
#    done
# >=2 open zones
    for m in 2; do
      for c in 4 3 2 1; do
        for z in 3; do
          file=$DIR/${s}/${m}_${c}_${repeat}_z${z}.result
          file2=$DIR/${s}/${m}_${c}_${z}_${repeat}.result
          echo "sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z ${z} > $file2"
#          if [[ ! -f $file && ! -f $file2 ]]; then
#            sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z ${z} > $file2
#            exit
#          fi
        done
      done
    done

# >=2 open zones, mixed request sizes
    for m in 1; do
#      for c in 1 2 3 4; do
      for c in 1 2 3 4; do
        for z in 1 2 3 4 5 6 7 8; do
          for r in 400 600 800 1000 1200 1400; do
#    for m in 2 3 4; do
#      for c in 2 4 6 8; do
#        for z in 2 3 4 5 6 7 8 9 10; do
            file=$DIR/${s}/${m}_${c}_${repeat}_z${z}.result
            file2=$DIR/${s}/${m}_${c}_${z}_r${r}_${repeat}.result
            echo "sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z ${z} -r ${r} > $file2"
            if [[ ! -f $file && ! -f $file2 ]]; then
              sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z ${z} -t ${r} > $file2 
            fi
          done
        done
      done
    done

    for m in 0; do
      for c in 1 2 4 8 16 32; do
        for z in 1 2 3 4 5 6 7 8; do
          for r in 400 600 800 1000 1200 1400; do
#    for m in 2 3 4; do
#      for c in 2 4 6 8; do
#        for z in 2 3 4 5 6 7 8 9 10; do
            file=$DIR/${s}/${m}_${c}_${repeat}_z${z}.result
            file2=$DIR/${s}/${m}_${c}_${z}_r${r}_${repeat}.result
            echo "sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z ${z} -r ${r} > $file2"
            if [[ ! -f $file && ! -f $file2 ]]; then
              sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z ${z} -t ${r} > $file2
            fi
          done
        done
      done
    done
  done
done

exit

for repeat in 0 1 2 3 4; do
  for s in 1 2 4 8 16; do
    mkdir -p $DIR/concurrent_reqs/${s}
    for m in 0; do
      for c in 1; do
        for z in 1 2 3 4 5 6 7 8; do
          file=$DIR/concurrent_reqs/${s}/${m}_${c}_${z}_${repeat}.result
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
          file=$DIR/concurrent_reqs/${s}/${m}_${c}_${z}_${repeat}.result
          echo "sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z ${z} > $file"
          if [[ ! -f $file || $(ls -s $file | awk '{print $1;}') -eq 0 ]]; then
            sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z ${z} > $file
          fi
        done
      done
    done
  done
done
