#!/bin/bash

config="zonewrite 1"
nqnid=0
nowait=""

if [[ $# -ge 1 ]]; then
  config=$1
fi
if [[ $# -ge 2 ]]; then
  nqnid=$2
fi
if [[ $# -ge 3 ]]; then
  nowait=$3
fi

SCRIPTS_DIR=$(realpath $(dirname $0))
source ${SCRIPTS_DIR}/../common.sh
cd ${SCRIPTS_DIR}

nqn=nqn.2016-06.io.spdk:cnode${nqnid}
#nqnid=$(echo $nqn | sed 's/cnode/ /g' | awk '{print $NF;}')
newnqnid=$(( $nqnid + 1 ))
newnqn=$(echo $nqn | sed "s/cnode$nqnid/cnode$newnqnid/g")
echo $newnqn

#######################################################
### The same as 3_reconnect_mount.sh after line 39 ####

./9_start_nvmf.sh "$config" $newnqn $nowait

if [[ $? -ne 0 ]]; then
  echo "failed to start nvmf"
  exit 1
fi
