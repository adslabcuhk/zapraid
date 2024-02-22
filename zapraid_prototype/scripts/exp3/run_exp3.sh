#!/bin/bash

CURRENT_DIR=$(realpath .)
SCRIPT_DIR=$(realpath $(dirname $0))
cd ${SCRIPT_DIR}

source ${SCRIPT_DIR}/../common.sh
DIR="${CURRENT_DIR}/exp3_compare_groupsize_new_read"

cp zns_raid_original.json zns_raid.json
json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.unit_size_str=\"8192\""

for try in 0 1 2 3 4; do 
for workload in write_only; do
  FIO_FILE="./conf/${workload}.fio"
  mode=2
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
  if [[ ! -d ${DIR}/mode_${mode} ]]; then
    mkdir -p ${DIR}/mode_${mode}
  fi
  for unit in 1 2 4; do
    for group_size in 4 8 16 32 64 128 256 512 1024 2048 4096; do
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.sync_group_size_str=\"${group_size}\""
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.unit_size_str=\"$(( $unit * 4096 ))\""
      sed -i "/\bbs/c\\bs=$(( ${unit} * 4 ))k" $FIO_FILE
      output_file=${DIR}/mode_${mode}/result_${unit}_$(( $unit * 4 ))k_${workload}_${group_size}_try${try}
      if [[ ! -f $output_file || $(ls -s $output_file | awk '{print $1;}') -eq 0 ]]; then
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev \
                             ${FIO_DIR}/fio ./conf/${workload}.fio > $output_file 
      fi
    done
  done
done
done

for workload in read_only; do
  FIO_FILE="./conf/${workload}.fio"
  mode=2
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.inject_degraded_read=1"
  if [[ ! -d ${DIR}/mode_${mode} ]]; then
    mkdir -p ${DIR}/mode_${mode}
  fi
  for unit in 1 2 4; do
    for group_size in 4 8 16 32 64 128 256 512 1024 2048 4096; do
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.unit_size_str=\"$(( $unit * 4096 ))\""
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.sync_group_size_str=\"${group_size}\""
      sed -i "/\bbs/c\\bs=$(( ${unit} * 4 ))k" $FIO_FILE
      output_file=${DIR}/mode_${mode}/result_${unit}_$(( $unit * 4 ))k_${workload}_${group_size}
      if [[ ! -f $output_file || $(ls -s $output_file | awk '{print $1;}') -eq 0 ]]; then
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev \
                             ${FIO_DIR}/fio ./conf/${workload}.fio | tee $output_file 
      fi
    done
  done
done
