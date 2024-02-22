#!/bin/bash

## Recommended: Load Mysql to the first before you start running tpcc-mysql


if [[ ! -d tpcc-mysql ]]; then
  git clone https://github.com/Percona-Lab/tpcc-mysql
  cd tpcc-mysql/src
  make
fi


