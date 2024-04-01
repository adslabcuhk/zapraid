# ZapRAID

## What's Inside
* `zapraid_prototype`, storing the prototype itself, implemented with C++
* `mysql`, the folder prepared for TPCC workload in MySQL
* `scripts`, the scripts to install the prototype and SPDK 
* `patches`, storing our modifications to SPDK and FEMU

## Building Guide
* Be sure to read this building guide before proceeding, and be sure to read every scripts before running it when doing experiments.

* ZapRAID SPDK (can also check `scripts/one_in_all_install.sh`)
    * Build `spdk` without zapraid first; configure with "--without-zapraid"
    * Build `zapraid_prototype`; specify the `SPDK_DIR` in src/CMakeLists.txt
        * `cd zapraid_prototype; cmake -Bbuild; cd build; make;`
    * Build `spdk` again with zapraid; configure with "--with-zapraid=DIR"
    * Check scripts in `scripts/` for running spdk fio and real applications

* ZapRAID FEMU
    * run `patches/cp_femu_files.sh` to clone the FEMU repository and put the modified files
    * Follow the [FEMU build guide](https://github.com/vtess/FEMU)

## Experiment scripts
* Check `zapraid_prototype/scripts`. 
    * Notes: Remember to format the SSDs (check `zapraid_prototype/scripts/format.sh`) and attach the SSDs to the SPDK (run `spdk/scripts/setup.sh`) before you run the experiments. 

## Explanations on the parameters

We have several parameters in the json files. Here is the explanations on the parameters. You do not need to change other parameters.

+ `block_size`: The block size of the ZNS SSD. It should be either 512 or 4096.
+ `unit_size_str`: The unit sizes of each open segment, delimited by `:`. For example, if there are two open segments with unit sizes of 4096 and 8192, the parameter should be `"4096:8192"`.
+ `num_blocks`: The total number of logical blocks in the ZNS RAID. 
+ `system_mode`: 0 for ZoneWrite-Only, 1 for ZoneAppend-Only, and 2 for ZapRAID.
+ `num_data_blocks`: The number of data chunks in a stripe, i.e., `k` for a `(k, m)` code.
+ `num_parity_blocks`: The number of parity chunks in a stripe, i.e., `m` for a `(k, m)` code.
+ `raid_level`: 0 for RAID-0, 5 for RAID-01, 2 for RAID-4, 3 for RAID-5, 4 for RAID-6.
+ `enable_gc`: 0 to disable GC, 1 to enable GC.
+ `sync_group_size_str`: The stripe group sizes of each open segment, delimited by `:`. For example, if there are two open segments with stripe group sizes of 256 and 1, the parameter should be `"256:1"`.
+ `num_open_segments`: The number of open segments.
+ `inject_degraded_read`: 0 to disable degraded reads, 1 to enable degraded read.
+ `enable_event_framework`: 0 to disable event framework, 1 to enable event framework. This parameter is set for the NVMe-oF object.
