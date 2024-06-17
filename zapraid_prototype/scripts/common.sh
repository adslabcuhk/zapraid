# please fill the below manually
#SPDK_DIR="$(realpath $(dirname $0))/../../spdk"
SPDK_DIR="/home/jhli/git_repos/zapraid_extension/spdk/"
FIO_DIR="/home/jhli/bins/fio-fio-3.35/"

if [[ ! -d $SPDK_DIR ]]; then
  SPDK_DIR="/home/femu/z_femu/spdk"
  FIO_DIR="/home/femu/fio-fio-3.35"
fi

export LD_LIBRARY_PATH=${SPDK_DIR}/dpdk/build/lib:${SPDK_DIR}/build/lib:$LD_LIBRARY_PATH

get_thpt_fio() {
  file=$1
  if [[ $file == "" ]]; then
      echo "Usage: $0 <file>"
      exit 1
  fi

# Get the throughput
  grep "WRITE.*MiB" $file | awk -F'=' '{print $2}' | sed 's/M/ /g' | awk '{print $1;}'
}

get_read_thpt_fio() {
  file=$1
  if [[ $file == "" ]]; then
      echo "Usage: $0 <file>"
      exit 1
  fi

# Get the throughput
  grep "READ.*MiB" $file | awk -F'=' '{print $2}' | sed 's/M/ /g' | awk '{print $1;}' | tr '\n' ','
}

get_p50_fio() {
  file=$1
  if [[ $file == "" ]]; then
      echo "Usage: $0 <file>"
      exit 1
  fi

  grep "50.00th" $file | head -n 1 | sed 's/]\|\[/ /g' | awk '{print $(NF-4);}' 
}

get_read_p50_fio() {
  file=$1
  if [[ $file == "" ]]; then
      echo "Usage: $0 <file>"
      exit 1
  fi

#  grep "50.00th" $file | awk '{if (NR>1) print; else s=$0;} END {if (NR==1) print s;}' | sed 's/]\|\[/ /g' | awk '{print $(NF-4);}' | tr '\n' ',' 
  grep "50.00th" $file | tail -n 5 | sed 's/]\|\[/ /g' | awk '{print $(NF-4);}' | tr '\n' ',' 
}

get_p95_fio() {
  file=$1
  if [[ $file == "" ]]; then
      echo "Usage: $0 <file>"
      exit 1
  fi

  grep "95.00th" $file | head -n 1 | sed 's/]\|\[/ /g' | awk '{print $(NF-1);}' 
}

get_read_p95_fio() {
  file=$1
  if [[ $file == "" ]]; then
      echo "Usage: $0 <file>"
      exit 1
  fi

#  grep "95.00th" $file | awk '{if (NR>1) print; else s=$0;} END {if (NR==1) print s;}' | sed 's/]\|\[/ /g' | awk '{print $(NF-1);}' | tr '\n' ',' 
  grep "95.00th" $file | tail -n 5 | sed 's/]\|\[/ /g' | awk '{print $(NF-1);}' | tr '\n' ',' 
}

config_json_segments() {
  filename=$1
  hybrid=$2

  if [[ $# -ne 2 ]]; then
    exit 1
  fi

  re='^[0-9]+$'
  if ! [[ ${hybrid:0:1} =~ $re ]] ; then
    echo "error: Not a number" >&2; 
    exit 1
  fi

  unit_size_str="4096"
  sync_group_size_str="1"
  num_open_segments="${hybrid:0:1}"

  suffix=${hybrid:1}
  if [[ "$suffix" =~ fns.$ || "$suffix" =~ fnl.$ || "$suffix" =~ f4kns.$ || "$suffix" =~ 4fknl.$ ]]; then  
    # standard ns+nl
    ns=${suffix: -1}
    nl=$(( $num_open_segments - $ns ))
    if [[ "$suffix" =~ fnl.$ || "$suffix" =~ f4knl.$ ]]; then
      nl=${suffix: -1}
      ns=$(( $num_open_segments - $nl ))
    fi

    small_unit_size=8192
    if [[ "$suffix" =~ .*4k.* ]]; then
      small_unit_size=4096
    fi

    if [[ $ns -eq 0 ]]; then
      unit_size_str="$(printf ":16384%.0s" $(seq 1 $nl))"
      sync_group_size_str="$(printf ":1%.0s" $(seq 1 $nl))"
      unit_size_str=${unit_size_str:1}
      sync_group_size_str=${sync_group_size_str:1}
    elif [[ $nl -eq 0 ]]; then
      unit_size_str="$(printf ":${small_unit_size}%.0s" $(seq 1 $ns))"
      unit_size_str=${unit_size_str:1}
      if [[ $ns -eq 1 ]]; then
        sync_group_size_str="256"
      else
        sync_group_size_str="256$(printf ":1%.0s" $(seq 1 $(( $ns - 1 ))))"
      fi 
    else
      unit_size_str="$(printf ":${small_unit_size}%.0s" $(seq 1 $ns))"
      unit_size_str="${unit_size_str}$(printf ":16384%.0s" $(seq 1 $nl))"
      unit_size_str=${unit_size_str:1}
      sync_group_size_str="256$(printf ":1%.0s" $(seq 1 $(( $nl + $ns - 1))))"
    fi
  elif [[ $hybrid == "7f16a0" ]]; then
    unit_size_str="16384:16384:16384:16384:16384:16384:16384"
    sync_group_size_str="1:1:1:1:1:1:1"
  elif [[ $hybrid == "7f32a0" ]]; then
    unit_size_str="32768:32768:32768:32768:32768:32768:32768"
    sync_group_size_str="1:1:1:1:1:1:1"
  elif [[ $hybrid == "8f8a1" ]]; then
    unit_size_str="8192:8192:8192:8192:8192:8192:8192:8192"
    sync_group_size_str="256:1:1:1:1:1:1:1"
  elif [[ "$hybrid" == "2f4k" ]]; then
    unit_size_str="4096:4096"
    sync_group_size_str="256:1"
  elif [[ "$hybrid" == "2f16k" ]]; then
    unit_size_str="16384:16384"
    sync_group_size_str="1:1"
  elif [[ "$hybrid" == "3f4k" ]]; then
    unit_size_str="4096:4096:4096"
    sync_group_size_str="256:1:1"
  elif [[ "$hybrid" == "4f4k" ]]; then
    unit_size_str="4096:4096:4096:4096"
    sync_group_size_str="256:1:1:1"
  elif [[ "$hybrid" == "5f4k" ]]; then
    unit_size_str="4096:4096:4096:4096:4096"
    sync_group_size_str="256:1:1:1:1"
  elif [[ "$hybrid" == "6f4k" ]]; then
    unit_size_str="4096:4096:4096:4096:4096:4096"
    sync_group_size_str="256:1:1:1:1:1"
  elif [[ "$hybrid" == "7f4k" ]]; then
    unit_size_str="4096:4096:4096:4096:4096:4096:4096"
    sync_group_size_str="256:1:1:1:1:1:1"
  elif [[ $hybrid == "8f8a8w16w16w16w16w16w16w" ]]; then
    unit_size_str="8192:8192:16384:16384:16384:16384:16384:16384"
    sync_group_size_str="256:1:1:1:1:1:1:1"
  elif [[ "$hybrid" == "8f4k" ]]; then
    unit_size_str="4096:4096:4096:4096:4096:4096:4096:4096"
    sync_group_size_str="256:1:1:1:1:1:1:1"
  elif [[ $hybrid == "7f8a1" ]]; then
    unit_size_str="8192:8192:8192:8192:8192:8192:8192"
    sync_group_size_str="256:1:1:1:1:1:1"
  elif [[ $hybrid == "7f8a8w16w16w16w16w16w" ]]; then
    unit_size_str="8192:8192:16384:16384:16384:16384:16384"
    sync_group_size_str="256:1:1:1:1:1:1"
  elif [[ $hybrid == "6f8a1" ]]; then
    unit_size_str="8192:8192:8192:8192:8192:8192"
    sync_group_size_str="256:1:1:1:1:1"
  elif [[ $hybrid == "6f8a8w16w16w16w16w" ]]; then
    unit_size_str="8192:8192:16384:16384:16384:16384"
    sync_group_size_str="256:1:1:1:1:1"
  elif [[ $hybrid == "5f8a1" ]]; then
    unit_size_str="8192:8192:8192:8192:8192"
    sync_group_size_str="256:1:1:1:1"
  elif [[ $hybrid == "5f8a8w16w16w16w" ]]; then
    unit_size_str="8192:8192:16384:16384:16384"
    sync_group_size_str="256:1:1:1:1"
  elif [[ $hybrid == "4f8a8w16a16w" ]]; then
    unit_size_str="8192:8192:16384:16384"
    sync_group_size_str="256:1:256:1"
  elif [[ $hybrid == "4f8a8w16a16a" ]]; then
    unit_size_str="8192:8192:16384:16384"
    sync_group_size_str="256:1:256:256"
  elif [[ $hybrid == "4f8a8a16w16w" ]]; then
    unit_size_str="8192:8192:16384:16384"
    sync_group_size_str="256:256:1:1"
  elif [[ $hybrid == "4f8a8a16a16w" ]]; then
    unit_size_str="8192:8192:16384:16384"
    sync_group_size_str="256:256:256:1"
  elif [[ $hybrid == "4f8a8w16a16a" ]]; then
    unit_size_str="8192:8192:16384:16384"
    sync_group_size_str="256:256:256:1"
  elif [[ $hybrid == "4f8a8w16w16w" ]]; then
    unit_size_str="8192:8192:16384:16384"
    sync_group_size_str="256:1:1:1"
  elif [[ $hybrid == "4f8a1" ]]; then
    unit_size_str="8192:8192:8192:8192"
    sync_group_size_str="256:1:1:1"
  elif [[ $hybrid == "4f8a16a16w16w" ]]; then
    unit_size_str="8192:16384:16384:16384"
    sync_group_size_str="256:256:1:1"
  elif [[ $hybrid == "4f8a16a16a16w" ]]; then
    unit_size_str="8192:16384:16384:16384"
    sync_group_size_str="256:256:256:1"
  elif [[ $hybrid == "4f8w16a16w16w" ]]; then
    unit_size_str="8192:16384:16384:16384"
    sync_group_size_str="1:256:1:1"
  elif [[ $hybrid == "4f8a8w8w16w" ]]; then
    unit_size_str="8192:8192:8192:16384"
    sync_group_size_str="256:1:1:1"
  elif [[ $hybrid == "4f8a8w8w16a" ]]; then
    unit_size_str="8192:8192:8192:16384"
    sync_group_size_str="256:1:1:256"
  elif [[ $hybrid == "4f8a8a8w16a" ]]; then
    unit_size_str="8192:8192:8192:16384"
    sync_group_size_str="256:256:1:256"
  elif [[ $hybrid == "4f8a1" ]]; then
    unit_size_str="8192:8192:8192:8192"
    sync_group_size_str="256:1:1:1"
  elif [[ $hybrid == "4f8a2" ]]; then
    unit_size_str="8192:8192:8192:8192"
    sync_group_size_str="256:256:1:1"
  elif [[ $hybrid == "4f8a3" ]]; then
    unit_size_str="8192:8192:8192:8192"
    sync_group_size_str="256:256:256:1"
  elif [[ $hybrid == "4f16a0" ]]; then
    unit_size_str="16384:16384:16384:16384"
    sync_group_size_str="1:1:1:1"
  elif [[ $hybrid == "4f16a1" ]]; then
    unit_size_str="16384:16384:16384:16384"
    sync_group_size_str="256:1:1:1"
  elif [[ $hybrid == "4f16a2" ]]; then
    unit_size_str="16384:16384:16384:16384"
    sync_group_size_str="256:256:1:1"
  elif [[ $hybrid == "4f16a3" ]]; then
    unit_size_str="16384:16384:16384:16384"
    sync_group_size_str="256:256:256:1"
  elif [[ $hybrid == "4f16a4" ]]; then
    unit_size_str="16384:16384:16384:16384"
    sync_group_size_str="256:256:256:256"
  elif [[ $hybrid == "7x16" ]]; then
    unit_size_str="16384:16384:16384:16384:16384:16384:16384"
    sync_group_size_str="256:256:256:256:256:256:256"
  elif [[ $hybrid == "3f8f16" ]]; then
    unit_size_str="8192:16384:16384"
    sync_group_size_str="256:256:256"
  elif [[ $hybrid == "3f16a0" ]]; then
    unit_size_str="16384:16384:16384"
    sync_group_size_str="1:1:1"
  elif [[ $hybrid == "3f16a1" ]]; then
    unit_size_str="16384:16384:16384"
    sync_group_size_str="256:1:1"
  elif [[ $hybrid == "3f16a2" ]]; then
    unit_size_str="16384:16384:16384"
    sync_group_size_str="256:256:1"
  elif [[ $hybrid == "3f8a2" ]]; then
    unit_size_str="8192:8192:8192"
    sync_group_size_str="256:256:1"
  elif [[ $hybrid == "3f8a1" ]]; then
    unit_size_str="8192:8192:8192"
    sync_group_size_str="256:1:1"
  elif [[ $hybrid == "3f8a16w16w" ]]; then
    unit_size_str="8192:16384:16384"
    sync_group_size_str="256:1:1"
  elif [[ $hybrid == "4f8a16w16w16w" ]]; then
    unit_size_str="8192:16384:16384:16384"
    sync_group_size_str="256:1:1:1"
  elif [[ $hybrid == "5f8a16w16w16w16w" ]]; then
    unit_size_str="8192:16384:16384:16384:16384"
    sync_group_size_str="256:1:1:1:1"
  elif [[ $hybrid == "6f8a16w16w16w16w16w" ]]; then
    unit_size_str="8192:16384:16384:16384:16384:16384"
    sync_group_size_str="256:1:1:1:1:1"
  elif [[ $hybrid == "7f8a16w16w16w16w16w16w" ]]; then
    unit_size_str="8192:16384:16384:16384:16384:16384:16384"
    sync_group_size_str="256:1:1:1:1:1:1"
  elif [[ $hybrid == "8f8a16w16w16w16w16w16w16w" ]]; then
    unit_size_str="8192:16384:16384:16384:16384:16384:16384:16384"
    sync_group_size_str="256:1:1:1:1:1:1:1"
  elif [[ $hybrid == "3f8a8w16w" ]]; then
    unit_size_str="8192:8192:16384"
    sync_group_size_str="256:1:1"
  elif [[ $hybrid == "4f8a8w16w16w" ]]; then
    unit_size_str="8192:8192:16384:16384"
    sync_group_size_str="256:1:1:1"
  elif [[ $hybrid == "5f8a8w16w16w16w" ]]; then
    unit_size_str="8192:8192:16384:16384:16384"
    sync_group_size_str="256:1:1:1:1"
  elif [[ $hybrid == "6f8a8w16w16w16w16w" ]]; then
    unit_size_str="8192:8192:16384:16384:16384:16384"
    sync_group_size_str="256:1:1:1:1:1"
  elif [[ $hybrid == "7f8a8w16w16w16w16w16w" ]]; then
    unit_size_str="8192:8192:16384:16384:16384:16384:16384"
    sync_group_size_str="256:1:1:1:1:1:1"
  elif [[ $hybrid == "8f8a8w16w16w16w16w16w16w" ]]; then
    unit_size_str="8192:8192:16384:16384:16384:16384:16384:16384"
    sync_group_size_str="256:1:1:1:1:1:1:1"
  elif [[ $hybrid == "3f8a8w8w" ]]; then
    unit_size_str="8192:8192:8192"
    sync_group_size_str="256:1:1"
  elif [[ $hybrid == "4f8a8w8w16w" ]]; then
    unit_size_str="8192:8192:8192:16384"
    sync_group_size_str="256:1:1:1"
  elif [[ $hybrid == "5f8a8w8w16w16w" ]]; then
    unit_size_str="8192:8192:8192:16384:16384"
    sync_group_size_str="256:1:1:1:1"
  elif [[ $hybrid == "6f8a8w8w16w16w16w" ]]; then
    unit_size_str="8192:8192:8192:16384:16384:16384"
    sync_group_size_str="256:1:1:1:1:1"
  elif [[ $hybrid == "7f8a8w8w16w16w16w16w" ]]; then
    unit_size_str="8192:8192:8192:16384:16384:16384:16384"
    sync_group_size_str="256:1:1:1:1:1:1"
  elif [[ $hybrid == "8f8a8w8w16w16w16w16w16w" ]]; then
    unit_size_str="8192:8192:8192:16384:16384:16384:16384:16384"
    sync_group_size_str="256:1:1:1:1:1:1:1"
  elif [[ $hybrid == "3f8a8a16w" ]]; then
    unit_size_str="8192:8192:16384"
    sync_group_size_str="256:256:1"
  elif [[ $hybrid == "2f8a1" ]]; then
    unit_size_str="8192:8192"
    sync_group_size_str="256:1"
  elif [[ $hybrid == "2f16" ]]; then
    unit_size_str="16384:16384"
    sync_group_size_str="256:256"
  elif [[ $hybrid == "2f16a1" ]]; then
    unit_size_str="16384:16384"
    sync_group_size_str="1:256"
  elif [[ $hybrid == "2f8f16" ]]; then
    unit_size_str="8192:16384"
    sync_group_size_str="256:256"
  elif [[ $hybrid == "2f8a16w" ]]; then
    unit_size_str="8192:16384"
    sync_group_size_str="256:1"
  elif [[ $hybrid == "2f16a0" ]]; then
    unit_size_str="16384:16384"
    sync_group_size_str="1:1"
  elif [[ $hybrid == "2f4f16" ]]; then
    unit_size_str="4096:16384"
    sync_group_size_str="256:256"
  elif [[ $hybrid == "1" ]]; then
    unit_size_str="$(( $unit * 4096 ))"
    sync_group_size_str="256"
  elif [[ $hybrid == "1f4" ]]; then
    unit_size_str="4096"
    sync_group_size_str="256"
  elif [[ $hybrid == "1f8" ]]; then
    unit_size_str="8192"
    sync_group_size_str="256"
  elif [[ $hybrid == "1f16" ]]; then
    unit_size_str="16384"
    sync_group_size_str="256"
  elif [[ $hybrid == "1f32" ]]; then
    unit_size_str="32768"
    sync_group_size_str="256"
  elif [[ $hybrid == "1f64" ]]; then
    unit_size_str="65536"
    sync_group_size_str="256"
  elif [[ $hybrid == "1f128" ]]; then
    unit_size_str="131072"
    sync_group_size_str="256"
  fi

  echo "unit_size_str: $unit_size_str"
  echo "sync_group_size_str: $sync_group_size_str"
  json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"${unit_size_str}\""
  json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"${sync_group_size_str}\""
  json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=${num_open_segments}"
}
