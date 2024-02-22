#!/bin/bash
CURRENT_DIR=$(realpath .)
SCRIPT_DIR=$(realpath $(dirname $0))
cd ${SCRIPT_DIR}

source ${SCRIPT_DIR}/../common.sh
DIR="${CURRENT_DIR}/exp5_recovery_test"

if [[ ! -d ${DIR} ]]; then
  mkdir ${DIR}
fi

for size in 100 200 300 400 500 600 700 800 900 1000;
do

  for unit in 1 2 4; do
    # Repeat reboot procedure (recover the system states)
    for repeat in 0 1 2 3 4;
    do
      output_file=${DIR}/reboot_${size}_${repeat}
      if [[ ! -f $output_file || $(ls -s $output_file | awk '{print $1;}') -eq 0 ]]; then
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL \
          ${CURRENT_DIR}/src/app -n 0 -m 2 -s ${size} -c 1 -h $unit
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL \
          ${CURRENT_DIR}/src/app -n 1 -m 2 -s ${size} -c 0 -h $unit > $output_file
      fi
    done

    # Repeat rebuild procedure
    sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL ${CURRENT_DIR}/src/app -n 0 -m 2 -s ${size} -c 0 -h $unit
    for repeat in 0 1 2 3 4; do
      output_file=${DIR}/rebuild_${size}_${repeat}
      if [[ ! -f $output_file || $(ls -s $output_file | awk '{print $1;}') -eq 0 ]]; then
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH stdbuf -oL \
          ${CURRENT_DIR}/src/app -n 2 -m 2 -s ${size} -c 0 -h $unit > $output_file 
          exit
      fi
    done
  done
done
