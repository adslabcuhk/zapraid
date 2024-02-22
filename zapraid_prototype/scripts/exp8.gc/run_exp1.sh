#!/bin/bash
# Need json tool

CURRENT_DIR=$(realpath .)
SCRIPT_DIR=$(realpath $(dirname $0))
cd ${SCRIPT_DIR}

source ${SCRIPT_DIR}/../common.sh
cp zns_raid_new.json zns_raid.json

DIR=${CURRENT_DIR}/exp8gcOptimized

if [[ ! -d "$DIR" ]]; then
  mkdir -p $DIR
fi
# GC

wss=200
mode=2

traffic=1024
#traffic=64

#for workload in 0 1 2 4; do
#  for size in 200 224 256 288 320; do
#    for sel in 3 0 1; do
for cnt in 0 1 2 3 4; do
for workload in 0 1 2 4 5 6 7; do
  for size in 224 256 288 320 200; do
    for sel in 4; do
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
      elif [[ $sel -eq 4 ]]; then  # 8+8+16+16
        h=4
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
      if [[ $tryNum -gt ${cnt} ]]; then
        continue
      fi

      if [[ $workload -eq 0 ]]; then  # 4KiB writes
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH  stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m ${mode} -s $size -g -w $wss -h ${h} -o ${o} -d 64 -r 1 -f ${traffic} > $outputfile
      elif [[ $workload -eq 1 ]]; then # 64KiB writes 
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m ${mode} -s $size -g -w $wss -h ${h} -o ${o} -d 64 -r 16 -f ${traffic} > $outputfile
      elif [[ $workload -eq 2 ]]; then # sequential write
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m ${mode} -s $size -g -w $wss -h ${h} -o ${o} -d 64 -r 16 -q -f ${traffic} > $outputfile
      elif [[ $workload -eq 3 ]]; then  # 16KiB random writes
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH  stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m ${mode} -s $size -g -w $wss -h ${h} -o ${o} -d 64 -r 4 -f ${traffic} > $outputfile
      elif [[ $workload -eq 4 ]]; then  # 64KiB skewed writes
        outdatafile="${SCRIPT_DIR}/../out_${wss}_${traffic}.data"
        if [[ ! -f ${outdatafile} ]]; then
          Rscript ${SCRIPT_DIR}/../gen.r 0.99 $(( $wss * 1024 * 256 )) $(( $traffic * 1024 * 256 )) 
          if [[ ! -f out.data ]]; then
            echo "out.data not found"
            exit
          fi
          echo "number of lines: $(wc -l out.data)"
          mv out.data ${outdatafile}
        fi
  
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH  stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m ${mode} -s $size -g -w $wss -h ${h} -o ${o} -d 64 -r 16 -f ${traffic} -k ${outdatafile} > $outputfile
      elif [[ $workload -eq 5 ]]; then # 16KiB random writes 
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m ${mode} -s $size -g -w $wss -h ${h} -o ${o} -d 64 -r 4 -f ${traffic} > $outputfile
      elif [[ $workload -eq 6 ]]; then # 16KiB sequential writes
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m ${mode} -s $size -g -w $wss -h ${h} -o ${o} -d 64 -r 4 -q -f ${traffic} > $outputfile
      elif [[ $workload -eq 7 ]]; then # 16KiB skewed writes
        outdatafile="${SCRIPT_DIR}/../out_${wss}_${traffic}.data"
        if [[ ! -f ${outdatafile} ]]; then
          Rscript ${SCRIPT_DIR}/../gen.r 0.99 $(( $wss * 1024 * 256 )) $(( $traffic * 1024 * 256 )) 
          if [[ ! -f out.data ]]; then
            echo "out.data not found"
            exit
          fi
          echo "number of lines: $(wc -l out.data)"
          mv out.data ${outdatafile}
        fi
  
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH  stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m ${mode} -s $size -g -w $wss -h ${h} -o ${o} -d 64 -r 4 -f ${traffic} -k ${outdatafile} > $outputfile
      fi
    done
  done
done
done
