#!/bin/bash
CURRENT_DIR=$(realpath .)
SCRIPT_DIR=$(realpath $(dirname $0))
cd ${SCRIPT_DIR}

source ${SCRIPT_DIR}/../common.sh
DIR="${CURRENT_DIR}/trace"
DIR="${CURRENT_DIR}/trace_small_l2p"

ORI_TRACE_DIR="/mnt/data/ali_traces/"
TRACE_DIR="/mnt/ramdisk/"

size=500
input="${SCRIPT_DIR}/volume_list.txt"
input="${SCRIPT_DIR}/volume_list_small_writes.txt"

DIR="${CURRENT_DIR}/trace_small_writes"
DIR="${CURRENT_DIR}/trace_small_writes_only12k"
DIR="${CURRENT_DIR}/trace_small_writes_taskset_only12k"
DIR="${CURRENT_DIR}/trace_small_writes_nolock"
if [[ ! -d ${DIR} ]]; then
  mkdir ${DIR}
fi

for cnt in 0 1 2 3 4; do
while read -r line; do
  echo "$line"
  rm -f ${TRACE_DIR}/*csv
  copied="false"

  volId=$(echo ${line} | cut -d'.' -f1)
  for mode in 0 1 2; do
    for sel in 0 1 2 3; do
#  for mode in 1; do # 0 1 2; do
#    for sel in 4; do
      h=4
      o=1
      if [[ $sel -eq 1 ]]; then
        h=1
        o=1
      elif [[ $sel -eq 2 ]]; then
        h=4
        o=4
      elif [[ $sel -eq 3 ]]; then
        h=6
        o=4
      elif [[ $sel -eq 4 ]]; then
        h=2
        o=1
      fi
      logPrefix="volume${volId}_${mode}_${h}_${o}"
      tryNum=0
#      outputfile="${DIR}/${logPrefix}_nolock_try${tryNum}.log"
      outputfile="${DIR}/${logPrefix}_try${tryNum}.log"
      while [[ -f "$outputfile" ]]; do
        if [[ $(grep "trace latency avg" $outputfile | wc -l) -lt 1 ]]; then
          # not run yet. Overwrite the log file
          break
        fi
        tryNum=$((tryNum+1))
#        outputfile="${DIR}/${logPrefix}_nolock_try${tryNum}.log"
        outputfile="${DIR}/${logPrefix}_try${tryNum}.log"
      done 
      echo outputfile $outputfile 
      if [[ $tryNum -gt $cnt ]]; then
        continue
      fi
      if [[ $copied == "false" ]]; then
        cp ${ORI_TRACE_DIR}/${line} ${TRACE_DIR}/
        copied="true"
      fi
#      taskset -c 12,13,14,15 sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m ${mode} -s ${size} -h ${h} -o ${o} -d 64 -t ${TRACE_DIR}/${line} > $outputfile 
      sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m ${mode} -s ${size} -h ${h} -o ${o} -d 64 -t ${TRACE_DIR}/${line} > $outputfile &

      sleep 1
#      ps -ef | grep "app" | grep -v grep | grep -v "sudo" | awk '{print $2}' | xargs sudo taskset -cp 9-15 # 12,13,14,15
      while [[ true ]]; do
        if [[ $(ps -ef | grep "app" | grep -v grep | grep -v "sudo" | wc -l) -eq 0 ]]; then
          break
        fi
        sleep 1
      done

      grep "Throughput" $outputfile | tail -n 1 

#      sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m ${mode} -s ${size} -h ${h} -o ${o} -d 64 -t ${TRACE_DIR}/${line} -a > $outputfile 
#      if [[ $? -ne 0 ]]; then
#        echo "Error: failed to run the program"
#        exit 1
#      fi
      sleep 5
    done
  done
#  exit
done < "$input"
done

exit
DIR="${CURRENT_DIR}/trace"
for cnt in 0 1 2 3 4; do
for l2psize in 250 100; do
while read -r line; do
  echo "$line"
  rm -f ${TRACE_DIR}/*csv
  copied="false"

  volId=$(echo ${line} | cut -d'.' -f1)
  for mode in 0 1 2; do
    for sel in 0 1 2 3; do
      h=4
      o=1
      if [[ $sel -eq 1 ]]; then
        h=1
        o=1
      elif [[ $sel -eq 2 ]]; then
        h=4
        o=4
      elif [[ $sel -eq 3 ]]; then
        h=6
        o=4
      fi
      logPrefix="volume${volId}_${mode}_${h}_${o}_l2p${l2psize}"
      tryNum=0
      outputfile="${DIR}/${logPrefix}_try${tryNum}.log"
      while [[ -f "$outputfile" ]]; do
        if [[ $(grep "trace latency avg" $outputfile | wc -l) -lt 1 ]]; then
          # not run yet. Overwrite the log file
          break
        fi
        tryNum=$((tryNum+1))
        outputfile="${DIR}/${logPrefix}_try${tryNum}.log"
      done 
      echo outputfile $outputfile 
      if [[ $tryNum -gt $cnt ]]; then
        continue
      fi
      if [[ $copied == "false" ]]; then
        cp ${ORI_TRACE_DIR}/${line} ${TRACE_DIR}/
        copied="true"
      fi
      sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m ${mode} -s ${size} -h ${h} -o ${o} -d 64 -t ${TRACE_DIR}/${line} -p ${l2psize} > $outputfile 
      if [[ $? -ne 0 ]]; then
        echo "Error: failed to run the program"
        exit 1
      fi
      sleep 5
    done
  done
done < "$input"
done
done
