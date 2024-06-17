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
RESULTS="raizn_test"

workload_init="write_only"

testing=""
#testing="true"

## test experiment
#for try in 0 1 2 3 4 5 6 7 8 9; do
for try in 0 1 2 3 4; do
#for usebssplit in "false" "true"; do
for usebssplit in "false"; do
#for usebssplit in "true"; do
for depth in 64; do 
  for hybrid in 1f16 2fns0 3fns0 4fns0 5fns0 6fns0 7fns0 8fns0; do 
    for workload in $workload_init; do
      for mode in 3 2; do 
        for unit in 4; do
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
                  ${FIO_DIR}/fio $FIO_FILE > $output_file

# o: num open segments; h: chunk size
#          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 \
#                               -m ${mode} -s 128 -g -w 12 -h 4 -o 1 -d 4 -r 4 -f 16 > $output_file
#          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 \
#                               -m ${mode} -s 128 -g -w 12 -h 4 -o 8 -d 64 -r 4 -f 64 > $output_file
#          if [[ $testing == "true" ]]; then
#              exit 
#          fi
        done
      done
    done
  done
done
done
done
#exit

## 4KiB writes
for try in 0 1 2 3 4; do
for usebssplit in "false"; do
for depth in 64; do 
  for hybrid in 1f4 2f4k 3f4k 4f4k 5f4k 6f4k 7f4k 8f4k; do 
#  for hybrid in 1f16; do 
    for workload in $workload_init; do
      for mode in 3 2; do 
        for unit in 1; do
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
                  ${FIO_DIR}/fio $FIO_FILE > $output_file

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

# hybrid writes
RESULT="raizn_test_hybrid_8_16"
for try in 0 1 2 3 4; do
for usebssplit in "true"; do
for depth in 64; do 
#  for hybrid in 2f16k 3f4knl2 4f4knl2 5f4knl2 6f4knl2 7f4knl2 8f4knl2 2f4k 3f4kns2 4f4kns2 5f4kns2 6f4kns2 7f4kns2 8f4kns2; do
  for hybrid in 2fns2 3fns2 4fns2 5fns2 6fns2 7fns2 8fns2 2fnl2 3fnl2 4fnl2 5fnl2 6fnl2 7fnl2 8fnl2 ; do
    for workload in $workload_init; do
      for mode in 3 2; do 
        for unit in 1; do 
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

          config_json_segments zns_raid.json $hybrid
          json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
          json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_blocks=$(( 1024 * 1024 * 1024 * 1024 / 4096 ))"  # 1TiB
          json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.enable_gc=1"

          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev \
                  ${FIO_DIR}/fio $FIO_FILE > $output_file

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

RESULTS="raizn_test_backup"
for try in 0 1 2 3 4; do
for usebssplit in "false"; do
for depth in 64; do 
  for hybrid in 1f8 2f8a1 3f8a1 4f8a1 5f8a1 6f8a1 7f8a1 8f8a1; do 
#  for hybrid in 1f16; do 
    for workload in $workload_init; do
      for mode in 3 2; do 
        for unit in 2; do
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
                  ${FIO_DIR}/fio $FIO_FILE > $output_file
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
