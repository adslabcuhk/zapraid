#!/bin/bash
CURRENT_DIR=$(realpath .)
SCRIPT_DIR=$(realpath $(dirname $0))
cd ${SCRIPT_DIR}

source ${SCRIPT_DIR}/../common.sh
DIR="${CURRENT_DIR}/exp4"

cp zns_raid_original.json zns_raid.json

for try in 0 1 2 3 4; do
  for mode in 0 2; do
    json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=${mode}"
    for qd in 64; do
      for unit in 1 2 4; do
        FIO_FILE="./conf/${qd}qd.fio"
        sed -i "/\bbs/c\\bs=$(( ${unit} * 4 ))k" $FIO_FILE
        if [[ ! -d ${DIR}/mode_${mode} ]]; then
          mkdir -p ${DIR}/mode_${mode}
        fi
        # RAID 0
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_data_blocks=4"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_parity_blocks=0"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.raid_level=0"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.unit_size_str=\"$(( ${unit} * 4096 ))\""
        output_file=${DIR}/mode_${mode}/result_raid0_${unit}_$(( ${unit} * 4 ))k_try${try}
        echo "output_file: $output_file"
        if [[ ! -f $output_file || $(grep "WRITE" ${output_file} | wc -l) -eq 0 ]]; then
          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}build/fio/spdk_bdev \
                               ${FIO_DIR}/fio ./conf/${qd}qd.fio > $output_file 
        fi

        # RAID 01
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_data_blocks=2"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_parity_blocks=2"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.raid_level=5"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.unit_size_str=\"$(( ${unit} * 4096 ))\""
        output_file=${DIR}/mode_${mode}/result_raid01_${unit}_$(( ${unit} * 4 ))k_try${try}
        echo "output_file: $output_file"
        if [[ ! -f $output_file || $(grep "WRITE" ${output_file} | wc -l) -eq 0 ]]; then
          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}build/fio/spdk_bdev \
                               ${FIO_DIR}/fio ./conf/${qd}qd.fio > ${output_file}
        fi

       # RAID 4
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_data_blocks=3"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_parity_blocks=1"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.raid_level=2"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.unit_size_str=\"$(( ${unit} * 4096 ))\""
        output_file=${DIR}/mode_${mode}/result_raid4_${unit}_$(( ${unit} * 4 ))k_try${try}
        echo "output_file: $output_file"
        if [[ ! -f $output_file || $(grep "WRITE" ${output_file} | wc -l) -eq 0 ]]; then
          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}build/fio/spdk_bdev \
                               ${FIO_DIR}/fio ./conf/${qd}qd.fio > $output_file 
        fi

        # RAID 5
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_data_blocks=3"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_parity_blocks=1"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.raid_level=3"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.unit_size_str=\"$(( ${unit} * 4096 ))\""
        output_file=${DIR}/mode_${mode}/result_raid5_${unit}_$(( ${unit} * 4 ))k_try${try}
        echo "output_file: $output_file"
        if [[ ! -f $output_file || $(grep "WRITE" ${output_file} | wc -l) -eq 0 ]]; then
          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}build/fio/spdk_bdev \
                               ${FIO_DIR}/fio ./conf/${qd}qd.fio > $output_file 
        fi

        # RAID 6
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_data_blocks=2"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_parity_blocks=2"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.raid_level=4"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.unit_size_str=\"$(( ${unit} * 4096 ))\""
        output_file=${DIR}/mode_${mode}/result_raid6_${unit}_$(( ${unit} * 4 ))k_try${try}
        echo "output_file: $output_file"
        if [[ ! -f $output_file || $(grep "WRITE" ${output_file} | wc -l) -eq 0 ]]; then
          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}build/fio/spdk_bdev \
                               ${FIO_DIR}/fio ./conf/${qd}qd.fio > $output_file 
        fi
      done
    done
  done
done
