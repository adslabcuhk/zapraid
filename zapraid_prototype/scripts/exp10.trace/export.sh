#!/bin/bash

../scripts/trace/check_throughput.sh trace/* | sed 's/_/ /g' | sed 's+trace/volume++g' | awk '{print $1 " " $2 " " $3 " " $4 " " $(NF-1);}'
