#!/bin/bash

SCRIPTS_DIR=$(realpath $(dirname $0))
source ${SCRIPTS_DIR}/../common.sh
cd ${SCRIPTS_DIR}

readFile=${SCRIPTS_DIR}/zapraid_control.txt

while true; do
  if [[ ! -f $readFile ]]; then
    sleep 60
    continue
  fi

  firstline=$(head -n 1 $readFile)
  nf=$(echo $firstline | awk '{print NF;}')
  item2=""
  if [[ $nf -ge 2 ]]; then
    item2=$(echo $firstline | awk '{print $2;}')
  fi
  item1=$(echo $firstline | awk '{print $1;}')
  if [[ $item1 == "zonewrite" ]]; then
    ${SCRIPTS_DIR}/0_start_spdk_nvmf_target.sh $item1 $item2
  elif [[ $item1 == "zoneappend" ]]; then
    ${SCRIPTS_DIR}/0_start_spdk_nvmf_target.sh $item1 $item2
  elif [[ $item1 == "zapraid" ]]; then
    ${SCRIPTS_DIR}/0_start_spdk_nvmf_target.sh $item1 $item2
  fi
  if [[ $? -ne 0 ]]; then
    echo "failed to start"
    exit
  fi
  echo "stopped"
  sleep 10
done
