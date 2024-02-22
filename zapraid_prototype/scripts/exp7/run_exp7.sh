#!/bin/bash
# Need json tool

CURRENT_DIR=$(realpath .)
SCRIPT_DIR=$(realpath $(dirname $0))
cd ${SCRIPT_DIR}

source ${SCRIPT_DIR}/../common.sh
cp zns_raid_new.json zns_raid.json

usebssplit="true"
RESULTS="exp7"
workload_init="write_only"

for try in 0 1 2 3 4; do
for usebssplit in "true" "false"; do
for depth in 64; do 
  for hybrid in 4f8a1 4f8a16w16w16w 4f8a8w16w16w 4f8a8w8w16w 4f16a0; do 
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

        if [[ $usebssplit != "true" ]]; then
          BS=$(( $unit * 4 ))k  # 4KiB, 8KiB, and 16KiB writes
          sed -i "/\bbs/c\\bs=${BS}" $FIO_FILE
          output_file=${CURRENT_DIR}/${RESULTS}/mode${mode}/result_unit${unit}_bs${BS}_depth${depth}_${hybrid_name}_try${try}
        else
          bsu=$(( $unit * 4 ))  # hybrid 4KiB and 16KiB writes
          if [[ $bsu -gt 4 ]]; then
            continue
          fi
          bssplit=${bsu}k/75:$(( $bsu * 4 ))k/25
          bssplit_name=$(echo $bssplit | sed 's/\///g' | sed 's/:/_/g')
          sed -i "/\bbs/c\\bssplit=${bssplit}" $FIO_FILE
          output_file=${CURRENT_DIR}/${RESULTS}/mode${mode}/result_unit${unit}_bssplit${bssplit_name}_depth${depth}_${hybrid_name}_try${try}
        fi
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
done
