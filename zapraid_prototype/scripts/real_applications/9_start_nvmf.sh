#!/bin/bash

SCRIPTS_DIR=$(realpath $(dirname $0))
source ${SCRIPTS_DIR}/../common.sh
cd ${SCRIPTS_DIR}

config=$1
newnqn=$2
nowait=$3

if [[ $config == "" ]]; then
  echo "config is not set"
  exit 1
fi
if [[ $newnqn == "" ]]; then
  echo "newnqn is not set"
  exit 1
fi

echo "$config" > ${SCRIPTS_DIR}/zapraid_control.txt

# Wait until the target is started 
set -x
while true; do
  echo "$(date) waiting for $config to start"
  if [[ $(ps -ef | grep stdbuf | grep -v grep | wc -l) -ne 0 ]]; then 
    break
  fi
  sleep 30 
done

if [[ $nowait == "" ]]; then
  echo "$(date) wait for 60 more seconds"
  sleep 60 
fi

## connect nvmf
sudo ${SPDK_DIR}/scripts/rpc.py nvmf_create_transport -t TCP -u 16384 -m 1 -c 8192
sudo ${SPDK_DIR}/scripts/rpc.py nvmf_create_subsystem ${newnqn} -a -s SPDK00000000000001 -d SPDK_Controller1
if [[ $? -ne 0 ]]; then
  echo "nvmf_create_subsystem failed"
  exit 1
fi
sudo ${SPDK_DIR}/scripts/rpc.py nvmf_subsystem_add_ns ${newnqn} ZnsRaid0
if [[ $? -ne 0 ]]; then
  echo "nvmf_subsystem_add_ns failed"
  exit 1
fi
sudo ${SPDK_DIR}/scripts/rpc.py nvmf_subsystem_add_listener ${newnqn} -t tcp -a 127.0.0.1 -s 4420
if [[ $? -ne 0 ]]; then
  echo "nvmf_subsystem_add_listener failed"
  exit 1
fi
sudo nvme discover -t tcp -a 127.0.0.1 -s 4420
if [[ $? -ne 0 ]]; then
  echo "nvme discover failed"
  exit 1
fi
sudo nvme connect -t tcp -n "${newnqn}" -a 127.0.0.1 -s 4420 --nr-io-queues 1 -k 0
if [[ $? -ne 0 ]]; then
  echo "nvme connect failed"
  exit 1
fi
sudo nvme list 2>/dev/null
sleep 1

## mount 
devname=$(sudo nvme list 2>/dev/null | tail -n 1 | awk '{print $1;}') 
sudo umount $devname
sudo mkfs.ext4 $devname
sudo mount $devname /mnt/zapraid_mount
sudo chown jhli:jhli /mnt/zapraid_mount
