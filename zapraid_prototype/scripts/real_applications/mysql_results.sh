#!/bin/bash

CURRENT_DIR=$(pwd)
schemes=("zonewrite" "zoneappend" "zapraid")
sels=(0 1 2 3)
for scheme in ${schemes[@]}; do
    for sel in ${sels[@]}; do
        printf "$scheme $(( $sel + 1)) "
        grep " TpmC" ${CURRENT_DIR}/mysql-results/${scheme}_${sel}* | awk '{st=$(NF-1) / 60 "," st;} END {print st;}'
    done
done 
