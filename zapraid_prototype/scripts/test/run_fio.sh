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
RESULTS="compare_hybrid_or_not_ctx16"
RESULTS="compare_4open_zones_fixed_balance"
RESULTS="compare_3open_zones_ctx4a64"
RESULTS="compare_3open_zones_ctx4a16"
RESULTS="compare_1open_zone_ctx4a16_new_footer"

verify=""
#verify="true"

workload_init="write_only"

if [[ $verify == "true" ]]; then
  RESULTS="verify"
  workload_init="write_only_verify"
fi

testing=""
#testing="true"

if [[ $verify == "true" ]]; then
  RESULTS="test"
fi
RESULTS="test_fio_new"

#json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256\""

#  for hybrid in 3f8f16a0 3f8f16a1 3f16 3f8 3f8a2 3f8a1; do # 2 ; do 
#  for hybrid in 4f8a8w16a16w 4f8a8w16a16a 4f8a8a16a16w 4f8a8w16a16a 4f8a8w16w16w 4f8a16a16w16w 4f8a16a16a16w 4f8w16a16w16w 4f8a8w8w16a 4f8a8a8w16a 4f8a1 4f8a2 4f8a3 4f16a1 4f16a2 4f16a3; do 
#  for hybrid in 4f8a8a16w16w; do 
#  for hybrid in 4f8a1 4f8a2 4f16a0 4f16a1; do 
#  for hybrid in 2f8a1 2f16a1 2f8a16w; do 
#  for hybrid in 3f16a0; do 
#  for hybrid in 3f16a1 3f8a1 3f8a2 3f8a16w16w 3f8a8a16w 3f8a8w16w; do # 2 ; do 
#  for hybrid in 2f8a16w; do 

# One open zone

#for try in 0 1 2 3 4 5 6 7 8 9; do
#for try in 0 1 2 3 4; do
for try in 0; do
#for usebssplit in "false" "true"; do
for usebssplit in "false"; do
for depth in 64; do #128 256; do
#  for hybrid in 4f8a8w8w16w; do 
#  for hybrid in 4f8a16w16w16w; do 
#  for hybrid in 4f8a8w8w16w; do 
#  for hybrid in 7f32a0; do 
#  for hybrid in 1f16; do 
  for hybrid in 4f16a0; do 
    for workload in $workload_init; do
      for mode in 2; do
#        for unit in 1; do
        for unit in 4; do
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

          json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_blocks=52428800"
          json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
          config_json_segments zns_raid.json $hybrid

          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev \
                  ${FIO_DIR}/fio $FIO_FILE > $output_file

          grep "WRITE.*MiB" $output_file
          sleep 1
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
