#!/bin/bash
SCRIPTS_DIR=$(realpath $(dirname $0))

source ${SCRIPTS_DIR}/common.sh
cd ${SCRIPTS_DIR}/../build/

sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/test_rw 
