# please fill the below manually
#SPDK_DIR="$(realpath $(dirname $0))/../../spdk"
SPDK_DIR="/home/jhli/git_repos/zapraid_extension/spdk/"
FIO_DIR="/home/jhli/bins/fio-fio-3.35/"

if [[ ! -d $SPDK_DIR ]]; then
  SPDK_DIR="/home/femu/zapraid_extension/spdk"
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
  if [[ $hybrid == "7f16a0" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"16384:16384:16384:16384:16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"1:1:1:1:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=7"
  elif [[ $hybrid == "7f32a0" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"32768:32768:32768:32768:32768:32768:32768\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"1:1:1:1:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=7"
  elif [[ $hybrid == "8f8a1" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:8192:8192:8192:8192:8192:8192\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:1:1:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=8"
  elif [[ $hybrid == "8f8a8w16w16w16w16w16w16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:16384:16384:16384:16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:1:1:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=8"
  elif [[ $hybrid == "7f8a1" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:8192:8192:8192:8192:8192\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:1:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=7"
  elif [[ $hybrid == "7f8a8w16w16w16w16w16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:16384:16384:16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:1:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=7"
  elif [[ $hybrid == "6f8a1" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:8192:8192:8192:8192\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=6"
  elif [[ $hybrid == "6f8a8w16w16w16w16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:16384:16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=6"
  elif [[ $hybrid == "5f8a1" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:8192:8192:8192\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=5"
  elif [[ $hybrid == "5f8a8w16w16w16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=5"
  elif [[ $hybrid == "4f8a8w16a16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:256:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f8a8w16a16a" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:256:256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f8a8a16w16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f8a8a16a16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256:256:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f8a8w16a16a" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256:256:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f8a8w16w16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f8a1" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:8192:8192\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f8a16a16w16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f8a16a16a16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256:256:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f8a16w16w16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f8w16a16w16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"1:256:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f8a8w8w16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:8192:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f8a8w8w16a" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:8192:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f8a8a8w16a" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:8192:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256:1:256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f8a1" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:8192:8192\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f8a2" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:8192:8192\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f8a3" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:8192:8192\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256:256:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f16a0" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"16384:16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"1:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f16a1" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"16384:16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f16a2" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"16384:16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f16a3" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"16384:16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256:256:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "4f16a4" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"16384:16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256:256:256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=4"
  elif [[ $hybrid == "7x16" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"16384:16384:16384:16384:16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256:256:256:256:256:256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=7"
  elif [[ $hybrid == "3f8f16" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256:256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=3"
  elif [[ $hybrid == "3f16a0" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"1:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=3"
  elif [[ $hybrid == "3f16a1" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=3"
  elif [[ $hybrid == "3f16a2" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"16384:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=3"
  elif [[ $hybrid == "3f8a2" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:8192\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=3"
  elif [[ $hybrid == "3f8a1" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:8192\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=3"
  elif [[ $hybrid == "3f8a16w16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=3"
  elif [[ $hybrid == "3f8a8w16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=3"
  elif [[ $hybrid == "3f8a8a16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=3"
  elif [[ $hybrid == "2f8a1" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:8192\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=2"
  elif [[ $hybrid == "2f16" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=2"
  elif [[ $hybrid == "2f16a1" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"1:256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=2"
  elif [[ $hybrid == "2f8f16" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=2"
  elif [[ $hybrid == "2f8a16w" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=2"
  elif [[ $hybrid == "2f16a0" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"16384:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"1:1\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=2"
  elif [[ $hybrid == "2f4f16" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"4096:16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256:256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=2"
  elif [[ $hybrid == "1" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"$(( $unit * 4096 ))\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=1"
  elif [[ $hybrid == "1f4" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"4096\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=1"
  elif [[ $hybrid == "1f8" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"8192\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=1"
  elif [[ $hybrid == "1f16" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"16384\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=1"
  elif [[ $hybrid == "1f32" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"32768\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=1"
  elif [[ $hybrid == "1f64" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"65536\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=1"
  elif [[ $hybrid == "1f128" ]]; then
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.unit_size_str=\"131072\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.sync_group_size_str=\"256\""
    json -I -f ${filename} -e "this.subsystems[0].config[0].params.num_open_segments=1"
  fi
}
