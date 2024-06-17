#!/bin/bash
# Need json tool

CURRENT_DIR=$(realpath .)
SCRIPT_DIR=$(realpath $(dirname $0))
cd ${SCRIPT_DIR}

modename=$1
if [[ $modename == "" ]]; then
  modename=2
fi

source ${SCRIPT_DIR}/../common.sh
cp zns_raid_new.json zns_raid.json

usebssplit="true"
RESULTS="raizn_debug"

workload_init="write_only"

#testing="true"

## test experiment
sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 \
                     -m 3 -s 128 -g -w 12 -h 6 -o 6 -d 4 -r 4 -f 64 > test_raizn_debug
exit
## 4KiB writes
for try in 0 1 2 3 4; do
for usebssplit in "false" "true"; do
for depth in 64; do 
#  for hybrid in 1f4 2f4k 3f4k 4f4k 5f4k 6f4k 7f4k 8f4k; do 
  for hybrid in 2fns0 1f8 3fnl2; do 
    for workload in $workload_init; do
      for mode in 2; do # 3; do 
        for unit in 1 4; do
          mkdir -p ${CURRENT_DIR}/${RESULTS}/mode${mode}
          FIO_FILE="./conf/tmp.fio"
          cp "./conf/${workload}.fio" $FIO_FILE

          sed -i "/\biodepth/c\\iodepth=${depth}" $FIO_FILE
          sed -i "/\bsize/c\\size=64g" $FIO_FILE
          sed -i "/\bramp_time/c\\ramp_time=2" $FIO_FILE

          hybrid_name="o${hybrid}"
          if [[ $mode -ne 2 ]]; then
            hybrid_name=$(echo $hybrid_name | sed 's/a[[:digit:]]\b//g' | sed 's/w\|a//g')
          fi

          if [[ $usebssplit != "true" ]]; then
            BS=$(( $unit * 4 ))k
            sed -i "/\bbs/c\\bs=${BS}" $FIO_FILE
            output_file=${CURRENT_DIR}/${RESULTS}/mode${mode}/result_unit${unit}_bs${BS}_depth${depth}_${hybrid_name}_try${try}
          else
            bsu=$(( $unit * 4 ))
            if [[ $bsu -gt 4 ]]; then
              continue
            fi
            bssplit=${bsu}k/75:$(( $bsu * 4 ))k/25
            bssplit_name=$(echo $bssplit | sed 's/\///g' | sed 's/:/_/g')
            sed -i "/\bbs/c\\bssplit=${bssplit}" $FIO_FILE
            output_file=${CURRENT_DIR}/${RESULTS}/mode${mode}/result_unit${unit}_bssplit${bssplit_name}_depth${depth}_${hybrid_name}_try${try}
          fi
          echo output_file $output_file
#          if [[ -f $output_file && $(ls -s $output_file | awk '{print $1;}') -ne 0 ]]; then
          if [[ -f $output_file && $(grep "WRITE" $output_file | wc -l) -ne 0 ]]; then
            continue
          fi

          json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
          json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_blocks=$(( 1024 * 1024 * 1024 * 1024 / 4096 ))"  # 1TiB
          json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.enable_gc=1"
          config_json_segments zns_raid.json $hybrid

          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev \
                  ${FIO_DIR}/fio $FIO_FILE > $output_file # 2&>1 #> $output_file

# o: num open segments; h: chunk size
#          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 \
#                               -m ${mode} -s 128 -g -w 12 -h 4 -o 1 -d 4 -r 4 -f 16 > $output_file
#          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 \
#                               -m ${mode} -s 128 -g -w 12 -h 4 -o 8 -d 64 -r 4 -f 64 > $output_file
          if [[ $testing == "true" ]]; then
              exit 
          fi
        done
      done
    done
  done
done
done
done
