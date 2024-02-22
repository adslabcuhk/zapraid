#!/bin/bash
CURRENT_DIR=$(realpath .)
SCRIPT_DIR=$(realpath $(dirname $0))
cd ${SCRIPT_DIR}

source ${SCRIPT_DIR}/../common.sh
DIR="${CURRENT_DIR}/exp6_scalability"
cp zns_raid_original.json zns_raid.json
writesize=64g
ramp_time=5

# femu
if [[ $(pwd | grep "femu" | wc -l) -ne 0 ]]; then
  echo "femu"
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_data_blocks=6"
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_parity_blocks=1"
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.enable_gc=1"
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_blocks=18874368"
fi

# please adapt this script if run on FEMU SSD; also, the device.cc in the source code needs changing (for zone capacity during Device creatio)
for try in 0 1 2 3 4; do
  for mode in 2; do
    mkdir -p ${DIR}/mode${mode}
    json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
    for qd in 4 8 16 32 64 128; do
      for hybrid in 1f4 1f8 1f16; do
        for unit in 1 2 4; do
          FIO_FILE="./conf/write_only.fio"
          chunkSize=$(echo $hybrid | sed 's/1f//g')
          if [[ $unit -ne $(( $chunkSize / 4 )) ]]; then
            continue
          fi
          sed -i "/\biodepth/c\\iodepth=${qd}" $FIO_FILE

          BS=$(( $unit * 4 ))k
          sed -i "/\bbs/c\\bs=${BS}" $FIO_FILE
          sed -i "/\bsize/c\\size=${writesize}" $FIO_FILE
          sed -i "/\bramp_time/c\\ramp_time=${ramp_time}" $FIO_FILE
          hybrid_name="o${hybrid}"
          output_file=${DIR}/mode${mode}/result_bs${BS}_depth${qd}_${hybrid_name}_try${try}
          echo output_file $output_file
          if [[ -f $output_file && $(grep "WRITE.*MiB" $output_file | wc -l) -ne 0 ]]; then
            echo "skip file $output_file"
            continue
          fi

          config_json_segments zns_raid.json $hybrid
          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev \
            ${FIO_DIR}/fio ${FIO_FILE} > ${output_file} 
          sleep 5
        done
      done
    done
  done
done
