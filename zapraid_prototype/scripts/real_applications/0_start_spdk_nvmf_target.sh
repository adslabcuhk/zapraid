#!/bin/bash
# Step 1: Start Target
CURRENT_DIR=$(realpath .)
SCRIPTS_DIR=$(realpath $(dirname $0))
source ${SCRIPTS_DIR}/../common.sh
cd ${SCRIPTS_DIR}

DIR="${CURRENT_DIR}/real/"
if [[ ! -f ${DIR} ]]; then
  mkdir -p ${DIR}
fi

sizeInBlks=$(( 256 * 1024 * 1024 ))  # 1024G 
scheme=$1

sel=0

if [[ $# -eq 2 ]]; then
  sel=$2
fi

filename=./conf/zns_raid_${scheme}.json
cp ${filename} zns_raid.json
cat ${filename}
json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_blocks=${sizeInBlks}"

if [[ $sel -eq 0 ]]; then # 16k
  chunkSize=16384
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.unit_size_str=\"${chunkSize}\""
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_open_segments=1"
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256\""
elif [[ $sel -eq 1 ]]; then # 4k
  chunkSize=4096
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.unit_size_str=\"${chunkSize}\""
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_open_segments=1"
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256\""
elif [[ $sel -eq 2 ]]; then ## hybrid 8*2+16*2
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:16384:16384\""
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_open_segments=4"
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:1\""
elif [[ $sel -eq 3 ]]; then ## hybrid 8+16*3
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:16384:16384:16384\""
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_open_segments=4"
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:1\""
fi

id=0
logname="${DIR}/${scheme}_${sel}_try${id}.log"
while [[ -f "$logname" ]]; do
  id=$(( id + 1 ))
  logname="${DIR}/${scheme}_${sel}_try${id}.log"
done

sudo stdbuf -oL ${SPDK_DIR}/build/bin/spdk_tgt -m 0x1fc -c zns_raid.json | \
       tee $logname 

if [[ $? -ne 0 ]]; then
  echo "failed to start"
  exit 1
fi
