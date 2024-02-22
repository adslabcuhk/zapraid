#!/bin/bash

CURRENT_DIR=$(pwd)
SCRIPT_DIR=$(realpath $(dirname $0))

label=$1
cnt=1

if [[ $# -ge 2 ]]; then
  cnt=$2
fi

if [[ ! -d $CURRENT_DIR/sysbench ]]; then
  mkdir $CURRENT_DIR/sysbench
fi

cd /mnt/zapraid_mount
if [[ ! -d sysbench ]]; then
  mkdir sysbench
fi
cd sysbench

date

## prepare
sysbench fileio --file-num=800 --file-total-size=100G --file-test-mode=rndwr --file-extra-flags=direct --file-io-mode=async --file-block-size=131072 prepare 

for ((i=0; i<${cnt}; i++)); do
  echo "count $i out of $cnt"
  trynum=0
  prefix="$CURRENT_DIR/sysbench/${label}_16kw"
  outputfile=${prefix}_try${trynum}.log
  while [[ -f $outputfile ]]; do
    trynum=$((trynum+1))
    outputfile=${prefix}_try${trynum}.log
  done
  echo output at $outputfile
  sysbench --threads=1 --time=600 fileio --file-num=800 --file-total-size=100G --file-test-mode=rndwr --file-extra-flags=direct --file-io-mode=async --file-fsync-freq=0 run | \
                     tee $outputfile 

  prefix="$CURRENT_DIR/sysbench/${label}_64kw"
  trynum=0
  outputfile=${prefix}_try${trynum}.log
  while [[ -f $outputfile ]]; do
    trynum=$((trynum+1))
    outputfile=${prefix}_try${trynum}.log
  done
  echo output at $outputfile
  sysbench --threads=1 --time=600 fileio --file-num=800 --file-total-size=100G --file-test-mode=rndwr --file-extra-flags=direct --file-io-mode=async --file-block-size=65536 --file-fsync-freq=0 run | \
                     tee $outputfile 
done

cd ../
rm -rf ./sysbench
