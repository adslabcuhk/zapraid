#include "raid_controller.h"
#include <sys/time.h>
#include "zns_raid.h"
#include <unistd.h>
#include <thread>
#include <chrono>
#include <thread>
#include <atomic>

#include <rte_errno.h>
#include "trace.h"

#include "sched.h"

// a simple test program to ZapRAID
uint64_t gSize = 64 * 1024 * 1024 / Configuration::GetBlockSize();
uint64_t gRequestSize = 4096;
bool gSequential = false;
bool gSkewed = false;
uint32_t gNumBuffers = 1024 * 128;
bool gCrash = false;
std::string gAccessTrace = "";

uint32_t gTestGc = 0; // 0: fill
bool gTestMode = false;
bool gUseLbaLock = false;
bool gHybridSize = false;
// 150GiB WSS for GC test
uint64_t gWss = 150ull * 1024 * 1024 * 1024 / Configuration::GetBlockSize(); 
uint64_t gTrafficSize = 1ull * 1024 * 1024 * 1024 * 1024;

uint32_t gChunkSize = 4096 * 4;
uint32_t gWriteSizeUnit = 16 * 4096; // 256KiB still stuck, use 64KiB and try

std::string gTraceFile = "";
struct timeval tv1;

RAIDController *gRaidController;
uint8_t *buffer_pool;

uint32_t qDepth = 1;
bool gVerify = false;
uint8_t* bitmap = nullptr;
uint32_t gNumOpenSegments = 1;
uint64_t gL2PTableSize = 0;
std::map<uint32_t, uint32_t> latCnt;

struct LatencyBucket
{
  struct timeval s, e;
  uint8_t *buffer;
  bool done;
  void print() {
    double elapsed = e.tv_sec - s.tv_sec + e.tv_usec / 1000000. - s.tv_usec / 1000000.;
    printf("%f\n", elapsed);
  }
};


void readCallback(void *arg) {
  LatencyBucket *b = (LatencyBucket*)arg;
  b->done = true;
}

void setdone(void *arg) {
  bool *done = (bool*)arg;
  *done = true;
}

void validate()
{
  uint8_t *readValidateBuffer = buffer_pool;
  char buffer[Configuration::GetBlockSize()];
  char tmp[Configuration::GetBlockSize()];
  printf("Validating...\n");
  struct timeval s, e;
  gettimeofday(&s, NULL);
//  Configuration::SetEnableDegradedRead(true);
  for (uint64_t i = 0; i < gSize; ++i) {
    LatencyBucket b;
    gettimeofday(&b.s, NULL);
    bool done;
    done = false;
    gRaidController->Read(
        i * Configuration::GetBlockSize(), Configuration::GetBlockSize(),
        readValidateBuffer + i % gNumBuffers * Configuration::GetBlockSize(),
        setdone, &done);
    while (!done) {
      std::this_thread::yield();
    }
    sprintf(buffer, "temp%lu", i * 7);
    if (strcmp(buffer, 
          (char*)readValidateBuffer + i % gNumBuffers * Configuration::GetBlockSize()) != 0) {
      printf("Mismatch %lu\n", i);
      assert(0);
      break;
    }
  }
  printf("Read finish\n");
//  gRaidController->Drain();
  gettimeofday(&e, NULL);
  double mb = (double)gSize * Configuration::GetBlockSize() / 1024 / 1024;
  double elapsed = e.tv_sec - s.tv_sec + e.tv_usec / 1000000. - s.tv_usec / 1000000.;
  printf("Throughput: %.2fMiB/s\n", mb / elapsed);
  printf("Validate successful!\n");
}

LatencyBucket *gBuckets;
static void latency_puncher(void *arg)
{
  LatencyBucket *b = (LatencyBucket*)arg;
  gettimeofday(&(b->e), NULL);
//  delete b->buffer;
//  b->print();
}

static void write_complete(void *arg)
{
  std::atomic<int> *ongoing = (std::atomic<int>*)arg;
  (*ongoing)--;
}

//uint64_t gLba = 0;
//
//static void write_complete_no_atomic(void *arg)
//{
//  uint64_t *finished = (uint64_t*)arg;
//  (*finished)++;
////  fprintf(stderr, "no atomic\n");
////
////  gRaidController->Write(gLba * 4096, 4096,
////      gBuckets[0].buffer,
////      write_complete_no_atomic, arg);
////  gLba += 1;
//}

struct trace_ctx {
//  std::atomic<int> ongoing;
//  uint64_t started;
//  uint64_t finished;
  std::atomic<uint64_t> started;
  std::atomic<uint64_t> finished;
  std::mutex mtx;
  std::map<uint64_t, int> lbaLock;
  std::map<uint64_t, int> lbaReadWrite;
}; // gTraceCtx;

struct trace_input_ctx {
  struct timeval tv;
  struct trace_ctx *ctx = nullptr;
  uint64_t lba; // in 4KiB
  uint32_t size; // in bytes
};

static void write_complete_trace(void *arg) {
  struct trace_input_ctx *ctx = (struct trace_input_ctx*)arg;
  struct timeval s, e;
  gettimeofday(&e, 0);
  if (gUseLbaLock) {
    ctx->ctx->mtx.lock();
    for (uint64_t dlba = 0; dlba < ctx->size / 4096; dlba++) {
      ctx->ctx->lbaLock[ctx->lba + dlba]--;
      if (ctx->ctx->lbaLock[ctx->lba + dlba] == 0) {
        ctx->ctx->lbaLock.erase(ctx->lba + dlba);
        ctx->ctx->lbaReadWrite.erase(ctx->lba + dlba);
      }
    }
    ctx->ctx->mtx.unlock();
  }
  ctx->ctx->finished++;
  uint64_t timeInUs = (e.tv_sec - ctx->tv.tv_sec) * 1000000 + e.tv_usec - ctx->tv.tv_usec;
  latCnt[timeInUs]++;
  delete ctx;
}

// lba in 4KiB
// size in bytes
struct trace_input_ctx* trace_ctx_add_lba(struct trace_ctx* tctx, uint64_t lba,
    uint32_t size, bool isWrite) {
  struct trace_input_ctx *ctx = new struct trace_input_ctx;
  // assignment
  ctx->ctx = tctx;
  ctx->lba = lba;
  ctx->size = size;
  gettimeofday(&ctx->tv, 0);

  // add lba
  if (gUseLbaLock) {
    ctx->ctx->mtx.lock();
    for (uint64_t dlba = 0; dlba < size / 4096; dlba++) {
      ctx->ctx->lbaLock[lba + dlba]++;
      ctx->ctx->lbaReadWrite[lba + dlba] = isWrite; 
    }
    ctx->ctx->mtx.unlock();
  }
  return ctx;
}

void testTrace() {
  latCnt.clear();
  uint32_t blockSize = Configuration::GetBlockSize();

  struct trace_ctx tctx;
  tctx.started = 0;
  tctx.finished = 0;
  uint64_t numLines = 0;

  Trace trace;
  std::filebuf fb;
  std::istream* ist;
  if (!fb.open(gTraceFile.c_str(), std::ios::in)) {
    std::cerr << "Input file error: " << gTraceFile << std::endl;
    exit(1);
  }
  ist = new std::istream(&fb);

  uint64_t lba, size, ts;
  bool isWrite;
  char line2[200];
  char* readBuffer = new char[gNumBuffers * blockSize];
  uint64_t bufferIdx, writtenBytes = 0;
  printf("file opened %s %p\n", gTraceFile.c_str(), ist);

  while (trace.readNextRequestFstream(*ist, ts, isWrite, lba, size, line2)) {
    // offset and length are in units of 4KiB blocks
    if (numLines % 1000000 == 0) {
      printf("trace %lu\n", numLines);
    }

    size *= blockSize;
    if (lba >= gSize) {
      printf("lba %lu >= gSize %lu\n", lba, gSize);
    }
    lba %= gSize;
    if (gTestMode) {
      if (size >= 16384) continue;
    }

    // get buffer
    assert(gNumBuffers >= size / blockSize);

    // try spliting the request because there is a bug
    uint64_t left = size;
    uint64_t wlba = lba;
    while (left > 0) {
      uint64_t wsize = (left >= gWriteSizeUnit) ? gWriteSizeUnit: left;

      bufferIdx = wlba % (gNumBuffers - wsize / blockSize + 1);
      gBuckets[bufferIdx].buffer = buffer_pool + bufferIdx * blockSize;

      while (tctx.started >= qDepth + tctx.finished) {
        std::this_thread::yield();
      }

      while (true) {
        bool start = true;
        if (gUseLbaLock) {
          tctx.mtx.lock();
          for (uint64_t dlba = 0; dlba < wsize / 4096; dlba++) {
            // the read and writes should be together; read and write should not multiplex
            if (tctx.lbaLock.count(wlba + dlba) && tctx.lbaReadWrite.count(wlba
                  + dlba) && tctx.lbaReadWrite[wlba + dlba] != isWrite)
            {
              start = false;
              break;
            }
          }
          tctx.mtx.unlock();
        }
        if (!start) {
          std::this_thread::yield();
        } else {
          break;
        }
      }

      struct trace_input_ctx* input_ctx = trace_ctx_add_lba(&tctx, wlba, wsize, isWrite);

      tctx.started++;
      if (isWrite) {
        gRaidController->Write(wlba * blockSize, wsize,
            gBuckets[bufferIdx].buffer,
            write_complete_trace, (void*)input_ctx);
      } else {
        gRaidController->Read(wlba * blockSize, wsize,
            readBuffer,
            write_complete_trace, (void*)input_ctx);
      }

      writtenBytes += wsize;
      wlba += wsize / blockSize;
      left -= wsize;
    }

    numLines++;

    if (numLines % 200000 == 100000) {
      struct timeval tv2;
      gettimeofday(&tv2, NULL);
      uint64_t timeInUs = (tv2.tv_sec - tv1.tv_sec) * 1000000 + tv2.tv_usec - tv1.tv_usec;
      printf("Read/Written %.2lf MiB, Throughput: %.2f MiB/s\n", writtenBytes / 1024.0 / 1024,
          (double)writtenBytes / timeInUs * 1000000 / 1024 / 1024);
//      printf("cpu: %d\n", sched_getcpu());
    }
  }

//  printf("started %lu finished %lu\n", tctx.started, tctx.finished);
  printf("started %lu finished %lu\n", tctx.started.load(), tctx.finished.load());
  struct timeval e1, e2;
  gettimeofday(&e1, NULL);
  while (tctx.started > tctx.finished) {
    gettimeofday(&e2, NULL);
    if (e2.tv_sec > e1.tv_sec) {
//      printf("started %lu finished %lu\n", tctx.started, tctx.finished);
      printf("started %lu finished %lu\n", tctx.started.load(), tctx.finished.load());
      e1 = e2;
    }
    std::this_thread::yield();
  }

  printf("--- wait for stats --- %lu lines\n", numLines);

  {
    struct timeval tv2;
    gettimeofday(&tv2, NULL);
    uint64_t timeInUs = (tv2.tv_sec - tv1.tv_sec) * 1000000 + tv2.tv_usec - tv1.tv_usec;
    printf("Read/Written %.2lf MiB, Throughput: %.2f MiB/s\n", writtenBytes / 1024.0 / 1024,
        (double)writtenBytes / timeInUs * 1000000 / 1024 / 1024);
  }

  DumpLatencies(latCnt, "trace latency");

  printf("--- read/write finished --- %lu lines\n", numLines);
}

void testGc() {
  uint32_t blockSize = Configuration::GetBlockSize();
  std::atomic<int> ongoing(0);
  uint64_t totalFinished = 0, totalStarted = 0;
  uint64_t writtenBytes = 0;

  // for sequential
  uint64_t lastLba = 0;

  // for skewed
  std::queue<uint64_t> lbas;
  const int queueSize = 1000000;
  bool fileFinished = false;
  std::string line;
  std::filebuf fb;
  std::istream *ist = nullptr;

  if (gSkewed) {
    if (!fb.open(gAccessTrace.c_str(), std::ios::in)) {
      std::cerr << "Input file error: " << gTraceFile << std::endl;
      exit(1);
    }
    ist = new std::istream(&fb);
    while (std::getline(*ist, line)) {
      lbas.push(std::stoull(line));
      if (lbas.size() > queueSize) {
        break;
      }
    }

    if (lbas.size() < queueSize) {
      fileFinished = true;
    }
  }

  while (writtenBytes < gTrafficSize) {
    // will change the size later
    uint32_t size = gRequestSize; // (rand() % 8 + 1) * blockSize; 
    if (gHybridSize && rand() % 4 == 3) {
      size *= 4; // hybrid 75% 4-KiB writes and 25% 16-KiB writes 
    }
    uint64_t lba;
    if (gSequential) {
      lba = (lastLba + gRequestSize / blockSize) % gWss;
      lastLba = lba;
    } else if (gSkewed) { 
      if (lbas.size() == 0) break;
      lba = lbas.front();
      assert(lba < gWss);
      lbas.pop();
    } else {
      lba = rand() % gWss;
      if (gWss - lba < size / blockSize) {
        lba = gWss - size / blockSize;
      }
    }

    uint32_t bufferIdx;
    if (gSkewed) {
      if (!fileFinished && lbas.size() < queueSize / 2) {
        while (std::getline(*ist, line)) {
          lbas.push(std::stoull(line));
          if (lbas.size() >= queueSize) {
            break;
          }
        }
        if (lbas.size() < queueSize) {
          fileFinished = true;
        }
      }
    }

    // put random numbers
    if (gVerify) {
      bufferIdx = lba;
      for (uint64_t wlba = lba; wlba < lba + size / blockSize; ++wlba) {
        bitmap[wlba] = 1;
        gBuckets[wlba].buffer = buffer_pool + wlba * blockSize;
        if (rand() % 256 < 8) {
          for (int blkIdx = 0; blkIdx < size; ++blkIdx) {
            gBuckets[bufferIdx].buffer[blkIdx] = rand() % 256;
          }
        }
      }
    } else {
      bufferIdx = lba % gNumBuffers;
      if (gNumBuffers - bufferIdx < size / blockSize) {
        bufferIdx = gNumBuffers - size / blockSize;
      }
      gBuckets[bufferIdx].buffer = buffer_pool + bufferIdx * blockSize;
    }

    while (ongoing >= qDepth) {
      std::this_thread::yield();
    }
    ongoing++;
//    while (totalStarted >= totalFinished + qDepth) {
//      std::this_thread::yield();
//    }
//    totalStarted++;

//    printf("write %lu %u buffer %p bufferIdx %d\n", lba, size,
//        gBuckets[bufferIdx].buffer, bufferIdx);
    gRaidController->Write(lba * blockSize, size,
        gBuckets[bufferIdx].buffer,
        write_complete, &ongoing);

    writtenBytes += size;

    if (writtenBytes / blockSize % 100000 == 0) {
      struct timeval tv2;
      gettimeofday(&tv2, NULL);
      uint64_t timeInUs = (tv2.tv_sec - tv1.tv_sec) * 1000000 + tv2.tv_usec - tv1.tv_usec;
      printf("GC %lu Written %.2lf MiB, Throughput: %.2f MiB/s\n",
          timeInUs, writtenBytes / 1024.0 / 1024, 
          (double)writtenBytes / timeInUs * 1000000 / 1024 / 1024);
    }
  }

  while (ongoing > 0) {
    std::this_thread::yield();
  }
//  while (totalFinished < totalStarted) {
//    std::this_thread::yield();
//  }

  {
    struct timeval tv2;
    gettimeofday(&tv2, NULL);
    uint64_t timeInUs = (tv2.tv_sec - tv1.tv_sec) * 1000000 + tv2.tv_usec - tv1.tv_usec;
    printf("GC %lu Written %.2lf MiB, Throughput: %.2f MiB/s\n",
        timeInUs, writtenBytes / 1024.0 / 1024, 
        (double)writtenBytes / timeInUs * 1000000 / 1024 / 1024);
  }
  printf("--- write finished --- (wss %lu)\n", gWss);

  if (gVerify) {
    uint8_t* readBuffer = (uint8_t*)spdk_zmalloc(
        blockSize, 4096, NULL, SPDK_ENV_SOCKET_ID_ANY,
        SPDK_MALLOC_DMA);

    for (uint64_t rlba = 0; rlba < gWss; ++rlba) {
      if (bitmap[rlba] == 0) {
        continue;
      }
      ongoing++;
      gRaidController->Read(rlba * blockSize, blockSize,
          readBuffer,
          write_complete, &ongoing);
      while (ongoing > 0) {
        std::this_thread::yield();
      }

      for (int j = 0; j < blockSize; ++j) {
        if (readBuffer[j] != gBuckets[rlba].buffer[j]) {
          printf("Mismatch at lba(%lu) %d read %d vs %d\n", rlba, j, 
              readBuffer[j], gBuckets[rlba].buffer[j]);
          assert(0);
        }
      }
    }
    spdk_free(readBuffer);
  }
  printf("--- verification finished ---\n");
  if (gSkewed) {
    delete ist;
  }
}

int main(int argc, char *argv[])
{
  setbuf(stderr, nullptr);
  uint32_t blockSize = Configuration::GetBlockSize();
  // Retrieve the options:
  int opt;
  while ((opt = getopt(argc, argv, "m:n:s:c:gw:d:vh:t:f:o:r:qk:p:al")) != -1) {  // for each option...
    switch (opt) {
      case 'm':
        Configuration::SetSystemMode(SystemMode(atoi(optarg)));
        break;
      case 'n':
        Configuration::SetRebootMode(atoi(optarg));
        break;
      case 's':
        gSize = atoi(optarg) * 1024ull * 1024 * 1024 / blockSize;
        break;
      case 'r':
        gRequestSize = atoi(optarg) * 4096;
        break;
      case 'q':
        gSequential = true;
        break;
      case 'k':
        gSkewed = true;
        gAccessTrace = optarg;
        break;
      case 'c':
        gCrash = atoi(optarg);
        break;
      case 'g':
        gTestGc = 1;
        break;
      case 'w':
        gWss = atoi(optarg) * 1024ull * 1024 * 1024 / blockSize;
        break;
      case 'd':
        qDepth = atoi(optarg);
        break;
      case 'v':
        gVerify = true;
        break;
      case 'h':
        gChunkSize = atoi(optarg) * 4096;
        break;
      case 't':
        gTraceFile = std::string(optarg);
        break;
      case 'f':
        gTrafficSize = atoi(optarg) * 1024ull * 1024 * 1024;
        break;
      case 'o':
        gNumOpenSegments = atoi(optarg);
        break;
      case 'p':
        gL2PTableSize = atoi(optarg) * 1024ull * 1024; // MiB
        break;
      case 'a':
        gTestMode = true;
        break;
      case 'l':
        gUseLbaLock = true;
        break;
      case 'y':
        gHybridSize = true;
        break;
      default:
        fprintf(stderr, "Unknown option %c\n", opt);
        break;
    }
  }
  if (gTestGc == 0 && gTraceFile == "") {
    Configuration::SetStorageSpaceInBytes(
        std::min(268435456ull, gSize * 2ull) * blockSize);
  } else {
    Configuration::SetStorageSpaceInBytes(gSize * blockSize);
  }

  Configuration::SetL2PTableSizeInBytes(gL2PTableSize);

  {
    Configuration::SetNumOpenSegments(gNumOpenSegments);
    uint32_t* groupSizes = new uint32_t[Configuration::GetNumOpenSegments()];
    uint32_t* unitSizes = new uint32_t[Configuration::GetNumOpenSegments()];
    int* paritySizes = new int[Configuration::GetNumOpenSegments()];
    int* dataSizes = new int[Configuration::GetNumOpenSegments()];
    for (int i = 0; i < Configuration::GetNumOpenSegments(); ++i) {
      groupSizes[i] = 256;
      unitSizes[i] = gChunkSize;
    }
    // special. 8+8+16+16
    if (gNumOpenSegments == 4 && gChunkSize == 16384) {
      groupSizes[1] = groupSizes[2] = groupSizes[3] = 1;
      unitSizes[0] = unitSizes[1] = 8192; 
    }

    // special. 8+8+8+16
    if (gNumOpenSegments == 4 && gChunkSize == 4096 * 5) {
      groupSizes[1] = groupSizes[2] = groupSizes[3] = 1;
      unitSizes[0] = unitSizes[1] = unitSizes[2] = 8192; 
      unitSizes[3] = 16384;
    }

    // special. 8+16+16+16
    if (gNumOpenSegments == 4 && gChunkSize == 4096 * 6) {
      groupSizes[1] = groupSizes[2] = groupSizes[3] = 1;
      unitSizes[0] = 8192; 
      unitSizes[1] = unitSizes[2] = unitSizes[3] = 16384;
    }

    // special. 8*4+16*2 
    if (gNumOpenSegments == 6 && gChunkSize == 4096 * 6) {
      groupSizes[1] = groupSizes[2] = groupSizes[3] = groupSizes[4] =
        groupSizes[5] = 1;
      unitSizes[0] = unitSizes[1] = unitSizes[2] = unitSizes[3] = 8192; 
      unitSizes[4] = unitSizes[5] = 16384;
    }
    for (int i = 0; i < Configuration::GetNumOpenSegments(); ++i) {
      paritySizes[i] = unitSizes[i];
      dataSizes[i] = 3 * unitSizes[i];
    }
    Configuration::SetStripeGroupSizes(groupSizes);
    Configuration::SetStripeUnitSizes(unitSizes);
    Configuration::SetStripeParitySizes(paritySizes);
    Configuration::SetStripeDataSizes(dataSizes);
  }

  {
    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.core_mask = "0x1fc";
    if (spdk_env_init(&opts) < 0) {
      fprintf(stderr, "Unable to initialize SPDK env.\n");
      exit(-1);
    }

    int ret = spdk_thread_lib_init(nullptr, 0);
    if (ret < 0) {
      fprintf(stderr, "Unable to initialize SPDK thread lib.\n");
      exit(-1);
    }
  }

  gRaidController = new RAIDController();
  gRaidController->Init(false);

  gBuckets = new LatencyBucket[gSize];

  // for write verification
  if (gVerify) {
    gNumBuffers = gWss;
    bitmap = new uint8_t[gNumBuffers]; 
    memset(bitmap, 0, gNumBuffers);
  }
  buffer_pool = new uint8_t[gNumBuffers * blockSize];
//  buffer_pool = (uint8_t*)spdk_zmalloc(
//      gNumBuffers * blockSize, 4096, NULL, SPDK_ENV_SOCKET_ID_ANY,
//      SPDK_MALLOC_DMA);
  if (buffer_pool == nullptr) {
    printf("Failed to allocate buffer pool\n");
    return -1;
  }

//  if (!gVerify) {
//    // by default, 512MiB data
//    for (uint64_t i = 0; i < (uint64_t)gNumBuffers * blockSize; ++i) {
//      if (i % (blockSize * gNumBuffers / 4) == 0) {
//        printf("Init %lu\n", i);
//      }
//      if (i % 4096 == 0) buffer_pool[i] = rand() % 256;
//    }
//  }

  gettimeofday(&tv1, NULL);
  uint64_t writtenBytes = 0;
  printf("Start writing...\n");

  if (Configuration::GetRebootMode() == 0) {
    if (gTraceFile != "") {
      testTrace();
    } else if (gTestGc) {
      testGc();
    } else {
      struct timeval s, e;
      gettimeofday(&s, NULL);
      uint64_t totalSize = 0;
      for (uint64_t i = 0; i < gSize; i += 1) {
        gBuckets[i].buffer = buffer_pool + i % gNumBuffers * blockSize;
        sprintf((char*)gBuckets[i].buffer, "temp%lu", i * 7);
        gettimeofday(&gBuckets[i].s, NULL);
        gRaidController->Write(i * blockSize,
            1 * blockSize,
            gBuckets[i].buffer,
            nullptr, nullptr);

        totalSize += 4096;
      }

      // Make a new segment of 100 MiB on purpose (for the crash recovery exps)
      uint64_t numSegs = gSize / (gRaidController->GetDataRegionSize() * 3);
      uint64_t toFill = 0;
      if (gSize % (gRaidController->GetDataRegionSize() * 3) > 100 * 256) {
        toFill = (numSegs + 1) * (gRaidController->GetDataRegionSize() * 3) + 100 * 256 - gSize;
      } else {
        toFill = gSize % (gRaidController->GetDataRegionSize() * 3);
      }

      for (uint64_t i = 0; i < toFill; i += 1) {
        gBuckets[i].buffer = buffer_pool + i % gNumBuffers * blockSize;
        sprintf((char*)gBuckets[i].buffer, "temp%lu", i * 7);
        gettimeofday(&gBuckets[i].s, NULL);
        gRaidController->Write(i * blockSize,
            1 * blockSize,
            gBuckets[i].buffer,
            nullptr, nullptr);

        totalSize += 4096;
      }

      if (gCrash) { // inject crash
        Configuration::SetInjectCrash();
        sleep(5);
      } else {
        gRaidController->Drain();
      }

      gettimeofday(&e, NULL);
      double mb = totalSize / 1024 / 1024;
      double elapsed = e.tv_sec - s.tv_sec + e.tv_usec / 1000000. - s.tv_usec / 1000000.;
      printf("Total: %.2f MiB, Throughput: %.2f MiB/s\n", mb, mb / elapsed);
    }
  }

//  validate();
  delete gRaidController;
}
