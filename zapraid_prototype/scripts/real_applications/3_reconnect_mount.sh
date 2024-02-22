#!/bin/bash

config="zonewrite 1"
if [[ $# -ge 1 ]]; then
  config=$1
fi

SCRIPTS_DIR=$(realpath $(dirname $0))
source ${SCRIPTS_DIR}/../common.sh
cd ${SCRIPTS_DIR}
devname=$(sudo nvme list 2>/dev/null | tail -n 1 | awk '{print $1;}') 
if [[ $(echo $devname | grep "nvme") == "" ]]; then
  echo "no nvme device found"
  exit 1
fi

## get information
#nqn=nqn.2016-06.io.spdk:cnode3
nqn=$(sudo nvme discover -t tcp -a 127.0.0.1 -s 4420 | grep "subnqn" | awk '{print $NF}')
nqnid=$(echo $nqn | sed 's/cnode/ /g' | awk '{print $NF;}')
newnqnid=$(( $nqnid + 1 ))
newnqn=$(echo $nqn | sed "s/cnode$nqnid/cnode$newnqnid/g")

nsid=$(sudo nvme list 2>/dev/null | tail -n 1 | awk '{print $4;}')

echo $nqn $nqnid $devname $nsid

# umount
sudo umount $devname

# disconnect
sudo nvme disconnect -n "${nqn}"

sudo ${SPDK_DIR}/scripts/rpc.py nvmf_subsystem_remove_listener ${nqn} -t tcp -a 127.0.0.1 -s 4420
sudo ${SPDK_DIR}/scripts/rpc.py nvmf_subsystem_remove_ns ${nqn} ${nsid} 
sudo ${SPDK_DIR}/scripts/rpc.py nvmf_delete_subsystem ${nqn}  

# kill stdbuf 
while [[ $(sudo ps -ef | grep spdk_tgt | grep -v grep | wc -l) -ne 0 ]]; do 
  sudo ps -ef | grep spdk_tgt | grep -v grep | head | awk '{print $2}' | sudo xargs kill -9
  sleep 1
done

./9_start_nvmf.sh "$config" $newnqn
if [[ $? -ne 0 ]]; then
  echo "failed to start nvmf"
  exit 1
fi
