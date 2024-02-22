#!/bin/bash

CURRENT_DIR=$(realpath .)
SCRIPT_DIR=$(realpath $(dirname $0))
cd ${SCRIPT_DIR}

source ${SCRIPT_DIR}/../common.sh
DIR="${CURRENT_DIR}/exp2"

cp zns_raid_original.json zns_raid.json

# Log-RAID degraded read
json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.inject_degraded_read=1"
for workload in read_only; do
  FIO_FILE="./conf/${workload}.fio"
  for mode in 0; do
    if [[ ! -d ${DIR}/mode_${mode} ]]; then
      mkdir -p ${DIR}/mode_${mode}
    fi
    for unit in 1 2 4; do
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.unit_size_str=\"$(( $unit * 4096 ))\""
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256\""
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_open_segments=1"
      output_file=${DIR}/mode_${mode}/result_${workload}_${unit}_$(( ${unit} * 4 ))k_static_mapping
      sed -i "/\bbs/c\\bs=$(( ${unit} * 4 ))k" $FIO_FILE

      if [[ ! -f $output_file || $(ls -s $output_file | awk '{print $1;}') -eq 0 ]]; then
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev \
                             ${FIO_DIR}/fio ${FIO_FILE} | tee $output_file
      fi
    done
  done
done

# Normal read
json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.inject_degraded_read=0"
for workload in read_only; do
  FIO_FILE="./conf/${workload}.fio"
  for mode in 2; do
    for unit in 1 2 4; do
      if [[ ! -d ${DIR}/mode_${mode} ]]; then
        mkdir -p ${DIR}/mode_${mode}
      fi
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.unit_size_str=\"$(( $unit * 4096 ))\""
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256\""
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_open_segments=1"
      output_file=${DIR}/mode_${mode}/result_${workload}_${unit}_$(( $unit * 4 ))k_normal_read_try0
      sed -i "/\bbs/c\\bs=$(( ${unit} * 4 ))k" $FIO_FILE

      if [[ ! -f $output_file || $(ls -s $output_file | awk '{print $1;}') -eq 0 ]]; then
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev \
                             ${FIO_DIR}/fio ./conf/${workload}.fio | tee $output_file
      fi
    done
  done
done

# ZapRAID degraded read
json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.inject_degraded_read=1"
for workload in read_only; do
  FIO_FILE="./conf/${workload}.fio"
  mode=2
  for unit in 1 2 4; do
    if [[ ! -d ${DIR}/mode_${mode} ]]; then
      mkdir -p ${DIR}/mode_${mode}
    fi
    json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
    json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.unit_size_str=\"$(( $unit * 4096 ))\""
    json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256\""
    json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_open_segments=1"
    output_file=${DIR}/mode_${mode}/result_${unit}_$(( $unit * 4 ))k_degraded_read_try0
    sed -i "/\bbs/c\\bs=$(( ${unit} * 4 ))k" $FIO_FILE
    if [[ ! -f $output_file || $(ls -s $output_file | awk '{print $1;}') -eq 0 ]]; then
      sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev \
                           ${FIO_DIR}/fio ./conf/${workload}.fio | tee $output_file
    fi
  done
done
