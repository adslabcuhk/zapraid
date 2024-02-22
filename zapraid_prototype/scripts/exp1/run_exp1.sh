#!/bin/bash
# Need json tool

CURRENT_DIR=$(realpath .)
SCRIPT_DIR=$(realpath $(dirname $0))
cd ${SCRIPT_DIR}

source ${SCRIPT_DIR}/../common.sh
cp zns_raid_new.json zns_raid.json

RESULTS="exp1"
workload_init="write_only"

for try in 0 1 2 3 4; do
for depth in 64; do 
  for hybrid in 1f4 1f8 1f16; do 
    for mode in 0 1 2; do
      for unit in 1 2 4; do
        mkdir -p ${CURRENT_DIR}/${RESULTS}/mode${mode}
        FIO_FILE="./conf/write_only.fio"

        sed -i "/\biodepth/c\\iodepth=${depth}" $FIO_FILE
        sed -i "/\bsize/c\\size=64g" $FIO_FILE
        sed -i "/\bramp_time/c\\ramp_time=2" $FIO_FILE

        hybrid_name="o${hybrid}"
        if [[ $mode -ne 2 ]]; then
          hybrid_name=$(echo $hybrid_name | sed 's/a[[:digit:]]\b//g' | sed 's/w\|a//g')
        fi

        BS=$(( $unit * 4 ))k
        sed -i "/\bbs/c\\bs=${BS}" $FIO_FILE
        output_file=${CURRENT_DIR}/${RESULTS}/mode${mode}/result_unit${unit}_bs${BS}_depth${depth}_${hybrid_name}_try${try}
        echo output_file $output_file
        if [[ -f $output_file && $(grep "WRITE" $output_file | wc -l) -ne 0 ]]; then
          continue
        fi

        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_blocks=$(( 1024 * 1024 * 1024 * 1024 / 4096 ))"
        json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.enable_gc=1"
        config_json_segments zns_raid.json $hybrid

        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev \
                ${FIO_DIR}/fio $FIO_FILE > $output_file
      done
    done
  done
done
done
