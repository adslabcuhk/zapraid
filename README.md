# ZapRAID

## What's Inside
* `zapraid_prototype`, storing the prototype itself, implemented with C++
* `mysql`, the folder prepared for TPCC workload in MySQL
* `scripts`, storing neccessary scripts for running all the experiments
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
