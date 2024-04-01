## Brief explanations on experiments

For more details, you are recommended to refer to our journal paper, which is still under review now.

Motivating experiments: 
+ The `exp0_motivating.sh` file is for motivating experiments. It considers 4KiB, 8KiB, and 16KiB requests from 1 to 8 open zones. 

+ Exp1: We evaluate the write performance of ZapRAID, by using FIO (v3.35) to generate a random write workload that issues random writes of 64GiB of data.
    + Part 1: Issue write requests of 4KiB, 8KiB, and 16KiB, and the request size equals to the chunk size.
    + Part 2: Issue write requests of 4KiB, and vary the chunk size between 4KiB, 8KiB, and 16KiB.
+ Exp2: We compare the performance of both normal reads and degraded reads in Log-RAID and ZapRAID.
    + We vary the chunk size and set the read size to be the same as the chunk size.
+ Exp3: We study the impact of the stripe group size G on the write and read performance in ZapRAID.
    + We vary G from 4 to 4,096. For both writes and reads, we vary the chunk size and set the request size to be the same as the chunk size.
+ Exp4: We configure RAID-0, RAID-01, RAID-4, RAID-5, and RAID-6 on the four ZNS SSDs (e.g., k=2 and m=2 in RAID-6). We compare ZapRAID and ZoneWrite-Only and set the request size to be the same as the chunk size.
+ Exp5: We evaluate the recovery performance of ZapRAID.
    + Part 1: Recovery from a system crash.
    + Part 2: Recovery from a full-drive failure.
+ Exp6: We examine the scalability of ZapRAID by evaluating its write performance under different values of queue depth.
    + We consider both real and FEMU-emulated ZNS SSDs. Note that the configurations can be the same for real and FEMU-emulated ZNS SSDs
+ Exp7: We evaluate the write performance ZapRAID on multiple open segments based on hybrid data management by varying the numbers of small-chunk and large-chunk segments.
+ Exp8: We evaluate the impact of garbage collection on ZapRAID with different reserved space sizes.
+ Exp9: We evaluate the overhead of offloading the L2P table entries to ZNS SSDs.
+ Exp10: We evaluate the performance of ZapRAID by trace-driven experiments.
