#!/bin/bash

SCRIPTS_PATH=$(realpath $(dirname $0))
cd $SCRIPTS_PATH/../

## 1. configure and compile spdk

git clone https://github.com/spdk/spdk spdk
cd spdk
git reset --hard 351271
git submodule update --init
./scripts/pkgdep.sh

## Apply patch
git apply ../patches/zapraid_spdk.patch
cp -R ../patches/zns_raid module/bdev/
./configure --without-zapraid
make
cd ..

## 2. configure prototype

cd zapraid_prototype/
cmake -Bbuild
cd build
make
cd ../../

## 3. configure and compile spdk again

cd spdk
./configure --with-zapraid="${SCRIPTS_PATH}/../zapraid_prototype/" --with-fio=/home/jhli/bins/fio-fio-3.35
make
