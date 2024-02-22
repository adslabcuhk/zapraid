#include "stats_recorder.h"
//#include <boost/concept_check.hpp>

StatsRecorder* StatsRecorder::mInstance = NULL;

unsigned long long inline timevalToMicros(struct timeval &res) {
  return res.tv_sec * 1000000ull + res.tv_usec;
}

long long unsigned int StatsRecorder::timeAddto(timeval& start_time, long long unsigned int& resTime)
{
    struct timeval end_time,res;
    unsigned long long diff;
    gettimeofday(&end_time,NULL);
    timersub(&end_time,&start_time,&res);
    diff = timevalToMicros(res);
    resTime += diff;
    return diff;
}


StatsRecorder* StatsRecorder::getInstance(){
    if(mInstance == NULL){
        mInstance  = new StatsRecorder();
    }
    return mInstance;
}

void StatsRecorder::DestroyInstance(){
    if(mInstance != NULL){
        delete mInstance;
        mInstance = NULL;
    }
}

StatsRecorder::StatsRecorder(){
    // init counters, e.g. bytes and time
    for(unsigned int i = 0 ; i < NUMLENGTH ; i++){
        time[i] = 0;
        total[i] = 0;
        max[i] = 0;
        min[i] = 1<<31;
        counts[i] = 0;
    }
    statisticsOpen = false;
    startGC = false;

    // init disk write bytes counters
    int N  = MAX_DISK;
    IOBytes.resize(N);
    for( int i = 0 ; i < N; i++){
        IOBytes[i] = std::pair<unsigned long long, unsigned long long> (0,0);
    }

    // init gc bytes stats
    unsigned long long segmentSize = 1048576; // ConfigManager::getInstance().getMainSegmentSize();
    int K  = MAX_DISK;
    // max log segment in a group
    unsigned long long factor = K * 8;
    // accuracy
    unsigned long long bytesPerSlot = 4096;
    unsigned long long numBuckets = segmentSize/bytesPerSlot*factor+1;
//    for (int i = 0; i < 2; i++) {
//        gcGroupBytesCount.valid.buckets[i] = new unsigned long long[numBuckets];
//        gcGroupBytesCount.invalid.buckets[i] = new unsigned long long[numBuckets];
//        gcGroupBytesCount.validLastLog.buckets[i] = new unsigned long long[numBuckets];
//        for (unsigned long long j = 0; j < numBuckets;j++) {
//            gcGroupBytesCount.valid.buckets[i][j] = 0;
//            gcGroupBytesCount.invalid.buckets[i][j] = 0;
//            gcGroupBytesCount.validLastLog.buckets[i][j] = 0;
//        }
//        gcGroupBytesCount.valid.sum[i] = 0;
//        gcGroupBytesCount.invalid.sum[i] = 0;
//        gcGroupBytesCount.validLastLog.sum[i] = 0;
//        gcGroupBytesCount.valid.count[i] = 0;
//        gcGroupBytesCount.invalid.count[i] = 0;
//        gcGroupBytesCount.validLastLog.count[i] = 0;
//    }
//    gcGroupBytesCount.bucketLen = numBuckets;
//    gcGroupBytesCount.bucketSize = bytesPerSlot;
    int maxGroup = 2; //ConfigManager::getInstance().getNumMainSegment() + 1;
    flushGroupCountBucketLen = segmentSize/512+1;
//    flushGroupCount.buckets[0] = new unsigned long long [maxGroup]; // data stripes in each flush
//    flushGroupCount.buckets[1] = new unsigned long long [flushGroupCountBucketLen]; // updates in each data stripe
//    for (int i = 0; i < maxGroup; i++) {
//        flushGroupCount.buckets[0][i] = 0;
//    }
//    for (int i = 0; i < flushGroupCountBucketLen; i++) {
//        flushGroupCount.buckets[1][i] = 0;
//    }
//    for (int i = 0; i < 2; i++) {
//        flushGroupCount.sum[i] = 0;
//        flushGroupCount.count[i] = 0;
//    }

//    _updateTimeHistogram = 0;
//    hdr_init(/* min = */ 1, /* max = */ (int64_t) 100 * 1000 * 1000 *1000, /* s.f. = */ 3, &_updateTimeHistogram);
//    _getTimeHistogram = 0;
//    hdr_init(/* min = */ 1, /* max = */ (int64_t) 100 * 1000 * 1000 *1000, /* s.f. = */ 3, &_getTimeHistogram);
}

void StatsRecorder::Dump() {
    // print all stats before destory
    fprintf(stdout, "==============================================================\n");

#define PRINT_SUM(_NAME_, _TYPE_) \
    do { \
        fprintf(stdout, "%-24s sum:%16llu\n", _NAME_, time[_TYPE_]); \
    } while (0);

#define PRINT_FULL(_NAME_, _TYPE_, _SUM_) \
    do { \
        fprintf(stdout, "%-24s sum:%16llu count:%12llu avg.:%10.2lf per.:%6.2lf%%\n", _NAME_, time[_TYPE_], counts[_TYPE_], time[_TYPE_]*1.0/counts[_TYPE_],time[_TYPE_] * 100.0 / _SUM_); \
    } while (0);

    fprintf(stdout,"------------------------- DELTAKV Temp  -------------------------------------\n");
    PRINT_FULL("Degraded read stripe", DEGRADED_READ_CSTABLE, time[DEGRADED_READ_CSTABLE]);
    PRINT_FULL("Flush stripe"   , FLUSH_CURRENT_STRIPE, time[FLUSH_CURRENT_STRIPE]);

    fprintf(stdout,"------------------------- Bytes Counters ------------------------------------\n");
    unsigned long long writeIOSum = 0, readIOSum = 0;
    for(int i = 0 ; i < MAX_DISK ; i++){
      if (IOBytes[i].first == 0 && IOBytes[i].second == 0) {
        continue;
      }
      fprintf(stdout,"Disk %5d                : (Write) %16llu (Read) %16llu\n",i,IOBytes[i].first,IOBytes[i].second);
      writeIOSum += IOBytes[i].first;
      readIOSum += IOBytes[i].second;
    }
    fprintf(stdout,"Total disk write          : %16llu\n",writeIOSum);
    fprintf(stdout,"Total disk read           : %16llu\n",readIOSum);

    printWriteLatInUs();

    fprintf(stdout, "==============================================================\n");
}

StatsRecorder::~StatsRecorder(){
  Dump();
}

uint64_t StatsRecorder::getTotalIOBytes() {
  uint64_t writeIOSum = 0, readIOSum = 0;
  for(int i = 0 ; i < MAX_DISK ; i++){
    writeIOSum += IOBytes[i].first;
    readIOSum += IOBytes[i].second;
  }
  return writeIOSum + readIOSum;
}

void StatsRecorder::totalProcess(StatsType stat, size_t diff, size_t count) {
    if(!statisticsOpen) return;

    total[stat] += diff;
    // update min and max as well
    if (counts[stat] == 0) {
        max[stat] = total[stat];
        min[stat] = total[stat];
    } else if (max[stat] < total[stat]) {
        max[stat] = total[stat];
    } else if (min[stat] > total[stat]) {
        min[stat] = total[stat];
    }
    counts[stat] += count;
}

unsigned long long StatsRecorder::timeProcess (StatsType stat, struct timeval &start_time, size_t diff, size_t count, unsigned long long valueSize) {
    unsigned long long ret = 0;
    if (!statisticsOpen) return 0;

    // update time spent 
    ret = timeAddto(start_time,time[stat]);

    // update total
    if (diff != 0) {
        totalProcess(stat, diff);
    } else {
        counts[stat] += count;
    }
    if (stat == StatsType::WRITE_LAT) {
      writeLatBucket[ret]++;
    }
    return ret;
}
  
void StatsRecorder::openStatistics(timeval& start_time)
{
    unsigned long long diff = 0;
    statisticsOpen  =  true;
    timeAddto(start_time,diff);
    fprintf(stdout,"Last Phase Duration :%llu us\n",diff);
}

unsigned long long StatsRecorder::getP999WriteLatInUs() {
    struct timeval tv;
    gettimeofday(&tv, 0); 
    if (tv.tv_sec > mWriteLatBucketTime) {
      mWriteLatBucketTime = tv.tv_sec;
      unsigned long long sum = 0, subSum = 0;
      for (auto it = writeLatBucket.begin(); it != writeLatBucket.end(); it++) {
        sum += it->second;
      }

      for (auto it = writeLatBucket.begin(); it != writeLatBucket.end(); it++) {
        subSum += it->second;
        if (subSum > 0.999 * sum) {
          if (it->first > 100) {
            mRecordedP999 = 100;
          } else {
            mRecordedP999 = it->first;
          }
          return mRecordedP999;
        }
      }

      return 10;
    }
    return mRecordedP999;
}

void StatsRecorder::printWriteLatInUs() {
  unsigned long long total_cnt = 0, total_time = 0;
  std::vector<double> percentiles = {50, 90, 95, 99, 99.9, 99.99};
  std::map<int, int> printed;
  unsigned long long cnt = 0;

  // append (in memory)
  for (auto it = writeLatBucket.begin(); it != writeLatBucket.end(); ++it) {
    total_cnt += it->second;
    total_time += (unsigned long long)it->first * it->second;
  }

  for (auto it = writeLatBucket.begin(); it != writeLatBucket.end(); ++it) {
    cnt += it->second;
    for (int i = 0; i < percentiles.size(); ++i) {
      auto p = percentiles[i];
      if (cnt >= total_cnt * p / 100 && !printed.count(i)) {
        printf("WriteLat p%.2f: %llu\n", p, it->first);
        printed[i] = 1;
        break;
      }
    }
  }

  printf("Write lat avg: %.2lf\n", (double)total_time / total_cnt);
}

uint64_t StatsRecorder::getTotalWriteBytes() {
  uint64_t writeIOSum = 0;
  for(int i = 0 ; i < MAX_DISK ; i++){
    writeIOSum += IOBytes[i].first;
  }
  return writeIOSum;
}
