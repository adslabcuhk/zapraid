#!/bin/bash
# Need json tool

CURRENT_DIR=$(realpath .)
SCRIPT_DIR=$(realpath $(dirname $0))
cd ${SCRIPT_DIR}

source ${SCRIPT_DIR}/../common.sh
cp zns_raid_new.json zns_raid.json

DIR=${CURRENT_DIR}/exp8gcOptimizedGcRead16k
DIR=${CURRENT_DIR}/test_gc/

if [[ ! -d "$DIR" ]]; then
  mkdir -p $DIR
fi
# GC

wss=50
mode=2

traffic=1024

for workload in 0; do
  for size in 60; do
    for sel in 0; do

### 16KiB writes on 16KiB chunks
#for workload in 3; do
#  for size in 320; do
#    for sel in 2; do
#for workload in 0; do
#  for size in 320; do
#    for sel in 3; do
      h=4
      o=1
      if [[ $sel -eq 1 ]]; then
        h=1
        o=1
      elif [[ $sel -eq 2 ]]; then
        h=6
        o=4
      elif [[ $sel -eq 3 ]]; then
        h=5
        o=4
      fi
      logPrefix="gc_${size}_${sel}"
      if [[ $workload -ne 0 ]]; then
        logPrefix="gcw${workload}_${size}_${sel}"
      fi

# get file name
      tryNum=0
      outputfile="${DIR}/${logPrefix}_try${tryNum}.log"
      while [[ -f "$outputfile" ]]; do
        if [[ $(grep "write finished" $outputfile | wc -l) -lt 1 ]]; then
          # not run yet. Overwrite the log file
          break
        fi
        tryNum=$((tryNum+1))
        outputfile="${DIR}/${logPrefix}_try${tryNum}.log"
      done
      echo outputfile $outputfile
      if [[ $tryNum -gt 5 ]]; then
        continue
      fi

      if [[ $workload -eq 0 ]]; then  # 4KiB writes
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH  stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m ${mode} -s $size -g -w $wss -h ${h} -o ${o} -d 64 -r 1 -f ${traffic} > $outputfile
      elif [[ $workload -eq 1 ]]; then # 64KiB writes 
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m ${mode} -s $size \
          -g -w $wss -h ${h} -o ${o} -d 64 -r 16 -f ${traffic} > $outputfile
      elif [[ $workload -eq 2 ]]; then # sequential write
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m ${mode} -s $size \
          -g -w $wss -h ${h} -o ${o} -d 64 -r 16 -q -f ${traffic} > $outputfile
      elif [[ $workload -eq 3 ]]; then  # 16KiB random writes
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH  stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m ${mode} -s $size -g -w $wss -h ${h} -o ${o} -d 64 -r 4 -f ${traffic} > $outputfile
      fi
      exit
    done
  done
done
