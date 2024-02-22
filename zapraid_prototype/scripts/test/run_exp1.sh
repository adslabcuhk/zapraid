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


verify=""
#verify="true"

gc=""
gc="true"

workload_init="write_only"
RESULTS="test"

if [[ $verify == "true" ]]; then
  RESULTS="verify2"
  workload_init="write_only_verify"
fi

if [[ $gc == "true" ]]; then
  RESULTS="gc"
  workload_init="gc"
fi

# GC
if [[ $gc == "true" ]]; then
  size=64
#  sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m 3 -s $size -g -w $(( $size / 2 )) > ${CURRENT_DIR}/apptest
#  sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m 3 -s $size -g -w $(( $size / 2 )) -d 1 -h 4 > ${CURRENT_DIR}/apptest
#  sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m 1 -s $size -g -w $(( $size / 2 )) -d 64 #-v #> ${CURRENT_DIR}/apptest
  sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m 1 -s $size -g -w $(( 48 )) -d 64 > ${CURRENT_DIR}/apptest #-v #> ${CURRENT_DIR}/apptest
  exit
fi

for try in 0; do
for usebssplit in "false"; do
for depth in 64; do #128 256; do
  for hybrid in 4f16a0; do 
    for workload in $workload_init; do
      for mode in 0 1 2; do
        for unit in 1; do
          mkdir -p ${CURRENT_DIR}/${RESULTS}/mode${mode}
          FIO_FILE="./conf/${workload}.fio"

          sed -i "/\biodepth/c\\iodepth=${depth}" $FIO_FILE

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
#            continue
#          fi

          json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
          json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_blocks=52428800"
          config_json_segments zns_raid.json $hybrid

          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev \
                  ${FIO_DIR}/fio $FIO_FILE > $output_file
#                  ${FIO_DIR}/fio $FIO_FILE | tee $output_file
          echo "output file $output_file"
          grep "WRITE.*MiB" $output_file
          exit 
        done
      done
    done
  done
done
done
done
