#!/bin/bash

SCRIPT_PATH=$(realpath $(dirname $0))

src="${SCRIPT_PATH}/femu_files"
dest="${SCRIPT_PATH}/../FEMU"
if [[ ! -d ${dest} ]]; then
  git clone https://github.com/vtess/FEMU $dest 
  cd ${dest}
  git reset --hard b388b40
  cd ${SCRIPT_PATH}
fi

# zns.c, zns_ftl.c, zns_ftl.h
cp ${src}/zns* ${dest}/hw/femu/zns/
# nvme-io.c
cp ${src}/nvme-io.c ${dest}/hw/femu/
# nvme.h
cp ${src}/nvme.h ${dest}/hw/femu/
# meson.build
cp ${src}/meson.build ${dest}/hw/femu/
# femu.c
cp ${src}/femu.c ${dest}/hw/femu/
# ftl.c
cp ${src}/ftl.c ${dest}/hw/femu/bbssd/
# run-zapraid.h 
cp ${src}/run-zapraid.sh ${dest}/femu-scripts/
# femu-copy-scripts.h
cp ${src}/femu-copy-scripts.sh ${dest}/femu-scripts/
