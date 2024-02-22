#pragma once
#include <stdio.h>
#include <sys/time.h>
#include <climits>
#include <cinttypes>
#include <unordered_set>
#include <unordered_map>
#include <map>
#include <vector>

//#include <hdr_histogram.h>
//#include "common/indexStorePreDefines.hpp"

#define S2US (1000 * 1000)
#define MAX_DISK (10)

enum StatsType {
    WRITE_LAT,
    DEGRADED_READ_CSTABLE,
    FLUSH_CURRENT_STRIPE,
    NUMLENGTH
};

class StatsRecorder {
 
public:
    static unsigned long long timeAddto(struct timeval &start_time,unsigned long long &resTime);

    static StatsRecorder *getInstance();
    static void DestroyInstance();

#define STAT_TIME_PROCESS(_FUNC_, _TYPE_) \
    do { \
        struct timeval startTime; \
        gettimeofday(&startTime, 0); \
        _FUNC_; \
        StatsRecorder::getInstance()->timeProcess(_TYPE_, startTime); \
    } while(0);

#define STAT_TIME_PROCESS_VS(_FUNC_, _TYPE_, _VS_) \
    do { \
        struct timeval startTime; \
        gettimeofday(&startTime, 0); \
        _FUNC_; \
        StatsRecorder::getInstance()->timeProcess(_TYPE_, startTime, 0, 1, _VS_); \
    } while(0);

    bool inline IsGCStart(){
        return startGC;
    }

    void totalProcess(StatsType stat, size_t diff, size_t count = 1);
    unsigned long long timeProcess(StatsType stat, struct timeval &start_time, size_t diff = 0, size_t count = 1, unsigned long long valueSize = 0);

    void inline IOBytesWrite(unsigned int bytes,unsigned int diskId){
        if (!statisticsOpen) return;
        IOBytes[diskId].first += bytes;
    }

    void inline IOBytesRead(unsigned int bytes,unsigned int diskId){
        if (!statisticsOpen) return;
        IOBytes[diskId].second += bytes;
    }

    void openStatistics(struct timeval &start_time);
    uint64_t getTotalIOBytes();
    void printProcess(const char* arg1,unsigned int i);

    // cannot be called very frequently
    unsigned long long getP999WriteLatInUs();
    void printWriteLatInUs();
    uint64_t getTotalWriteBytes();
    void Dump();

private:
    StatsRecorder(); 
    ~StatsRecorder();
  
    static StatsRecorder* mInstance;

    bool statisticsOpen;
    bool startGC;
    unsigned long long time[NUMLENGTH];
    unsigned long long total[NUMLENGTH];
    unsigned long long max[NUMLENGTH];
    unsigned long long min[NUMLENGTH];
    unsigned long long counts[NUMLENGTH];
    unsigned long long gcNums;
    unsigned long long gcInMemNums;
    unsigned long long us;
    unsigned long long lsmLookupTime;
    unsigned long long approximateMemoryUsage;
    std::vector<std::pair<unsigned long long, unsigned long long>> IOBytes; /* write,read */

    int flushGroupCountBucketLen;

    // for write latency
    std::map<unsigned long long, unsigned int> writeLatBucket;
    unsigned long long mWriteLatBucketTime = 0;
    unsigned long long mRecordedP999 = 10;

    enum {
        MAIN,
        LOG
    };
};

