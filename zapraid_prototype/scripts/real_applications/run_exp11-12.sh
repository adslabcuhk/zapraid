#!/bin/bash

CURRENT_DIR=$(realpath .)
SCRIPTS_DIR=$(realpath $(dirname $0))
source ${SCRIPTS_DIR}/../common.sh

modes=("zonewrite" "zoneappend" "zapraid")
sels=("0" "1" "2")
sels=("1" "0" "2" "3")

## make sure you run the following command to start nvmf object
#${SCRIPTS_DIR}/3_1_start_nvmf.sh "zoneappend 2" "3"
#sleep 10

first="true"

# sysbench benchmarks
for cnt in 0 1 2 3 4; do
  for sel in ${sels[@]}; do
    for mode in ${modes[@]}; do
      if [[ $first == "false" ]]; then
        ${SCRIPTS_DIR}/3_reconnect_mount.sh "${mode} ${sel}"
        ret=$?
        if [[ $ret -ne 0 ]]; then
          echo "error: $ret"
          exit 1
        fi
      fi
      first="false"

      sysbenchcnt=1
      ${SCRIPTS_DIR}/sysbench.sh ${mode}_sel${sel} ${sysbenchcnt}
      ret=$?
      if [[ $ret -ne 0 ]]; then
        echo "error: $ret"
        exit 1
      fi
    done
  done
done

# mysql benchmarks
for cnt in 0 1 2 3 4; do
  for sel in ${sels[@]}; do
    for mode in ${modes[@]}; do
      if [[ $first == "false" ]]; then
        ${SCRIPTS_DIR}/3_reconnect_mount.sh "${mode} ${sel}"
        ret=$?
        if [[ $ret -ne 0 ]]; then
          echo "error: $ret"
          exit 1
        fi
      fi
      first="false"

      ${SCRIPTS_DIR}/mysql_start.sh
      ret=$?
      if [[ $ret -ne 0 ]]; then
        echo "error: $ret"
        exit 1
      fi
    done
  done
done
