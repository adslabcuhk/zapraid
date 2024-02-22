#!/bin/bash

CURRENT_DIR=$(realpath .)
SCRIPTS_DIR=$(realpath $(dirname $0))
source ${SCRIPTS_DIR}/../common.sh

# the mysql database after loaded
ORIG_DIR="/mnt/data/mysql_loaded/"
# the mount point of ZapRAID
ZAPRAID_DIR="/mnt/zapraid_mount/mysql/"

num=$(df -hl | grep zapraid_mount | grep "nvme" | wc -l)
if [[ $num -eq 0 ]]; then
  echo "zapraid not mounted"
  exit 1
fi

DIR=${CURRENT_DIR}/mysql-results/
if [[ ! -d ${DIR} ]]; then
    mkdir -p ${DIR}
fi
fname="${DIR}/$(cat ${SCRIPTS_DIR}/zapraid_control.txt | sed 's/ /_/g')"
id=0
while [[ -f "${fname}_${id}.log" ]]; do 
  if [[ $(grep "TpmC" ${fname}_${id}.log | wc -l) -lt 2 ]]; then
    break
  fi 
  id=$((id+1))
done

if [[ $id -ge 5 ]]; then
  echo "too many files"
  exit
fi

# prepare
if [[ $# -eq 0 ]]; then
  sudo cp -R $ORIG_DIR $ZAPRAID_DIR 
  sudo chmod -R 777 $ZAPRAID_DIR
fi
sudo systemctl status mysql
sudo systemctl start mysql
if [[ $? -ne 0 ]]; then
  echo "start mysql failed"
  exit 1
fi

# run
cd ${SCRIPTS_DIR}/../../../mysql/tpcc-mysql/

fnamefull="${fname}_${id}.log"

sudo iostat -x -d 1 > ${fname}_${id}_iostat.log &
iostat_pid=$!

echo "output file: ${fnamefull}"
./tpcc_start -h127.0.0.1 -P3306 -dtpcc200 -uroot -w1000 -c32 -r10 -l3600 -p root39- | tee ${fnamefull} 

sudo kill $iostat_pid

# stop
sudo systemctl stop mysql
