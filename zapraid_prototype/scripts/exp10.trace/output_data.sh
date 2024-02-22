#!/bin/bash

echo "throughput:"
grep "Written" $* | tac | sort -u -t: -k1,1 | sed 's/_/ /g' | sed 's+trace new hybrid/volume++g' | awk '{print $1 " " $2 " " $3 " " $4 " " $(NF-1);}'

#echo "p95:"
#grep "trace latency p95.00" $* | tac | sort -u -t: -k1,1 | sed 's/_/ /g' | sed 's+trace/volume++g' | awk '{print $1 " " $2 " " $3 " " $4 " " $NF;}'
#grep "Written" $* | tac | sort -u -t: -k1,1 | awk '{s+=$2;} END {print s / 1024;}' 

echo "p99:"
grep "trace latency p99.00" $* | tac | sort -u -t: -k1,1 | sed 's/_/ /g' | sed 's+trace new hybrid/volume++g' | awk '{print $1 " " $2 " " $3 " " $4 " " $NF;}'
