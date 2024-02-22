#!/bin/bash
# Step 3: format and mount bdev

SCRIPTS_DIR=$(realpath $(dirname $0))
source ${SCRIPTS_DIR}/../common.sh
cd ${SCRIPTS_DIR}

sudo umount $1
sudo mkfs.ext4 $1
sudo mount $1 /mnt/zapraid_mount
# change username to your own user
#sudo chown username:username /mnt/zapraid_mount
sudo chown jhli:jhli /mnt/zapraid_mount
