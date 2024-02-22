#!/bin/bash
# Step 2: create NVMe-oF transport

SCRIPTS_DIR=$(realpath $(dirname $0))
source ${SCRIPTS_DIR}/../common.sh
cd ${SCRIPTS_DIR}

nodeid=0

if [[ $# -ge 1 ]]; then
  nodeid=$1
fi

sudo ${SPDK_DIR}/scripts/rpc.py nvmf_create_transport -t TCP -u 16384 -m 1 -c 8192
sudo ${SPDK_DIR}/scripts/rpc.py nvmf_create_subsystem nqn.2016-06.io.spdk:cnode${nodeid} -a -s SPDK00000000000001 -d SPDK_Controller1
sudo ${SPDK_DIR}/scripts/rpc.py nvmf_subsystem_add_ns nqn.2016-06.io.spdk:cnode${nodeid} ZnsRaid0
sudo ${SPDK_DIR}/scripts/rpc.py nvmf_subsystem_add_listener nqn.2016-06.io.spdk:cnode${nodeid} -t tcp -a 127.0.0.1 -s 4420
sudo nvme discover -t tcp -a 127.0.0.1 -s 4420
sudo nvme connect -t tcp -n "nqn.2016-06.io.spdk:cnode${nodeid}" -a 127.0.0.1 -s 4420 --nr-io-queues 1 -k 0 # -v
sudo nvme list

## sudo nvme disconnect -n "nqn.2016-06.io.spdk:cnode0"
## sudo ${SPDK_DIR}/scripts/rpc.py nvmf_subsystem_remove_listener nqn.2016-06.io.spdk:cnode0 -t tcp -a 127.0.0.1 -s 4420
## sudo ${SPDK_DIR}/scripts/rpc.py nvmf_subsystem_remove_ns nqn.2016-06.io.spdk:cnode0 1 
## sudo ${SPDK_DIR}/scripts/rpc.py nvmf_delete_subsystem nqn.2016-06.io.spdk:cnode0  

#### check listeners:
## sudo ${SPDK_DIR}/scripts/rpc.py nvmf_subsystem_get_listeners nqn.2016-06.io.spdk:cnode1
#### check qpairs
## sudo scripts/rpc.py nvmf_subsystem_get_qpairs nqn.2016-06.io.spdk:cnode1

######### nsid: check "NameSpace" in `sudo nvme list`
