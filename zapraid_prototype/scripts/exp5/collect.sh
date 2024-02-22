#!/bin/bash

DIR=$1

for time in 100 200 300 400 500 600 700 800 900 1000; do
  str="$time 4 "
  for tryNum in 0 1 2 3 4; do
    writeTime=$(grep "Restart time" $DIR/reboot_${time}_${tryNum} | awk '{print $NF;}') 
    str="${str}${writeTime}"
    if [[ $tryNum -ne 4 ]]; then
      str="${str},"
    fi
  done
  echo $str
done

echo ""
for time in 100 200 300 400 500 600 700 800 900 1000; do
  str="$time 4 "
  for tryNum in 0 1 2 3 4; do
    writeTime=$(grep "Rebuild time" $DIR/rebuild_${time}_${tryNum} | awk '{print $NF;}') 
    str="${str}${writeTime}"
    if [[ $tryNum -ne 4 ]]; then
      str="${str},"
    fi
  done
  echo $str
done
