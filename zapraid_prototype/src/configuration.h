#ifndef ___CONFIG_H___
#define ___CONFIG_H___

#include <string>

enum SystemMode {
  ZONEWRITE_ONLY, ZONEAPPEND_ONLY, ZAPRAID, RAIZN_SIMPLE
};

enum RAIDLevel {
  RAID0, RAID1, RAID4, RAID5, RAID6,
  // For a four-drive setup, RAID01 first stripes the blocks into two drives,
  // and then replicate the two blocks into four drives during parity generation
  RAID01,
};

const static int raid6_4drive_mapping[4][4] = {
  {0, 1, 2, 3},
  {0, 2, 3, 1},
  {2, 3, 0, 1},
  {3, 0, 1, 2}
};

const static int raid6_5drive_mapping[5][5] = {
  {0, 1, 2, 3, 4},
  {0, 1, 3, 4, 2},
  {0, 3, 4, 1, 2},
  {3, 4, 0, 1, 2},
  {4, 0, 1, 2, 3}
};

const static int raid6_6drive_mapping[6][6] = {
  {0, 1, 2, 3, 4, 5},
  {0, 1, 2, 4, 5, 3},
  {0, 1, 4, 5, 2, 3},
  {0, 4, 5, 1, 2, 3},
  {4, 5, 0, 1, 2, 3},
  {5, 0, 1, 2, 3, 4}
};

struct StripeConfig {
  int size = 16384;
  int dataSize = 4096 * 3;
  int paritySize = 4096 * 1;
  int unitSize = 4096 * 1;
  int groupSize = 256;
};

class Configuration {
public:
  static Configuration& GetInstance() {
    static Configuration instance;
    return instance;
  }

  static void PrintConfigurations() {
    Configuration &instance = GetInstance();
    const char *systemModeStrs[] = {"ZoneWrite-Only", "ZoneAppend-Only", "ZapRAID", "RAIZN-Simple"};
    printf("ZapRAID Configuration:\n");
    printf("-- Block size: %d\n", instance.GetBlockSize());
    for (int i = 0; i < instance.gNumOpenSegments; i++) {
      printf("-- Raid mode: %d %d %d %d %d | %d--\n",
          instance.gStripeConfig[i].size,
          instance.gStripeConfig[i].dataSize,
          instance.gStripeConfig[i].paritySize,
          instance.gStripeConfig[i].unitSize,
          instance.gStripeConfig[i].groupSize,
          instance.gRaidScheme);
    }
    printf("-- System mode: %s --\n", systemModeStrs[(int)instance.gSystemMode]);
    printf("-- GC Enable: %d --\n", instance.gEnableGc);
    printf("-- Framework Enable: %d --\n", instance.gEnableEventFramework);
    printf("-- Storage size: %lu -- (%lu GiB)\n",
        instance.gStorageSpaceInBytes, instance.gStorageSpaceInBytes / 1024 /
        1024 / 1024);
  }

  // compatible with the original design
  static int GetStripeSize(int index = 0) {
    return GetInstance().gStripeConfig[index].size;
  }

  static int GetStripeDataSize(int index = 0) {
    return GetInstance().gStripeConfig[index].dataSize;
  }

  static int GetStripeParitySize(int index = 0) {
    return GetInstance().gStripeConfig[index].paritySize;
  }

  static int GetStripeUnitSize(int index = 0) {
    return GetInstance().gStripeConfig[index].unitSize;
  }

  static int GetMaxStripeUnitSize() {
    int max = 0;
    for (int i = 0; i < GetInstance().gNumOpenSegments; i++) {
      auto& conf = GetInstance().gStripeConfig[i];
      if (conf.unitSize > max) {
        max = conf.unitSize;
      }
    }
    return max;
  }

  static void SetBlockSize(int blockSize) {
    GetInstance().gBlockSize = blockSize;
  }

  static int GetBlockSize() {
    return GetInstance().gBlockSize;
  }

  static int GetMetadataSize() {
    return GetInstance().gMetadataSize;
  }

  static int GetNumIoThreads() {
    return GetInstance().gNumIoThreads;
  }

  static bool GetDeviceSupportMetadata() {
    return GetInstance().gDeviceSupportMetadata;
  }

  static void SetDeviceSupportMetadata(bool flag) {
    GetInstance().gDeviceSupportMetadata = flag;
  }

  static SystemMode GetSystemMode() {
    return GetInstance().gSystemMode;
  }

  static void SetSystemMode(SystemMode mode) {
    GetInstance().gSystemMode = mode;
  }

  static int GetStripePersistencyMode() {
    return GetInstance().gStripePersistencyMode;
  }

  static void SetStripeDataSizes(int* stripeDataSizes) {
    for (int i = 0; i < GetInstance().gNumOpenSegments; i++) {
      auto& conf = GetInstance().gStripeConfig[i];
      conf.dataSize = stripeDataSizes[i];
      conf.size = conf.dataSize + conf.paritySize;
    }
  }

  static void SetStripeParitySizes(int* stripeParitySizes) {
    for (int i = 0; i < GetInstance().gNumOpenSegments; i++) {
      auto& conf = GetInstance().gStripeConfig[i];
      conf.paritySize = stripeParitySizes[i];
      conf.size = conf.dataSize + conf.paritySize;
    }
  }

  static void SetStripeDataSize(int stripeDataSize) {
    auto& conf = GetInstance().gStripeConfig[0];
    conf.dataSize = stripeDataSize;
    conf.size = conf.dataSize + conf.paritySize;
  }

  static void SetStripeParitySize(int stripeParitySize) {
    auto& conf = GetInstance().gStripeConfig[0];
    conf.paritySize = stripeParitySize;
    conf.size = conf.dataSize + conf.paritySize;
  }

  static void SetEnableGc(bool enable) {
    GetInstance().gEnableGc = enable;
  }

  static bool GetEnableGc() {
    return GetInstance().gEnableGc;
  }

  static void SetStripeUnitSizes(uint32_t* unitSizes) {
    for (int i = 0; i < GetInstance().gNumOpenSegments; i++) {
      auto& conf = GetInstance().gStripeConfig[i];
      conf.unitSize = unitSizes[i];
    }
  } 

  // compatible with the default call
  static int GetStripeGroupSize(int index = 0) {
    return GetInstance().gStripeConfig[index].groupSize;
  }

  // make sure that the data region of all segments are aligned
  static int GetMaxStripeGroupSizeInBlocks() {
    int max = 0;
    for (int i = 0; i < GetInstance().gNumOpenSegments; i++) {
      auto& conf = GetInstance().gStripeConfig[i];
      if (conf.groupSize * conf.unitSize / GetBlockSize() > max) {
        max = conf.groupSize * conf.unitSize / GetBlockSize();
      }
    }
    return max;
  }

  static int GetLargeRequestThreshold() {
    return GetInstance().gLargeRequestThreshold;
  }

  static void SetStripeGroupSizes(uint32_t* groupSizes) {
    for (int i = 0; i < GetInstance().gNumOpenSegments; i++) {
      auto& conf = GetInstance().gStripeConfig[i];
      conf.groupSize = groupSizes[i];
    }
  }

  static void SetStripeGroupSize(uint32_t groupSize) {
    auto& conf = GetInstance().gStripeConfig[0];
    conf.groupSize = groupSize;
  }

  static void SetEnableDegradedRead(bool enable) {
    GetInstance().gEnableDegradedRead = enable;
  }

  static bool GetEnableDegradedRead() {
    return GetInstance().gEnableDegradedRead;
  }

  static bool GetEnableIOLatencyTest() {
    return GetInstance().gEnableIOLatencyTest;
  }

  static void SetRaidLevel(RAIDLevel level) {
    GetInstance().gRaidScheme = level;
  }

  static RAIDLevel GetRaidLevel() {
    return GetInstance().gRaidScheme;
  }

  static void SetNumOpenSegments(uint32_t num_open_segments) {
    if (GetInstance().gNumOpenSegments != num_open_segments) {
      int min_open_seg = std::min(GetInstance().gNumOpenSegments,
          num_open_segments);
      int max_open_seg = std::max(GetInstance().gNumOpenSegments,
          num_open_segments);
      auto ptr = GetInstance().gStripeConfig;
      GetInstance().gStripeConfig = new StripeConfig[num_open_segments];
      for (int i = 0; i < min_open_seg; i++) {
        GetInstance().gStripeConfig[i] = ptr[i];
      }
      if (ptr != nullptr) {
        delete[] ptr;
      }
    }
    GetInstance().gNumOpenSegments = num_open_segments;
  }

  static int GetNumOpenSegments() {
    return GetInstance().gNumOpenSegments;
  }

  static void SetEnableHeaderFooter(bool enable_header_footer) {
    GetInstance().gEnableHeaderFooter = enable_header_footer;
  }

  static bool GetEnableHeaderFooter() {
    return GetInstance().gEnableHeaderFooter;
  }

  static bool GetBypassDevice() {
    return false;
  }

  static void SetRebootMode(uint32_t rebootMode) {
    GetInstance().gRebootMode = rebootMode;
  }

  static uint32_t GetRebootMode() {
    return GetInstance().gRebootMode;
  }

  static uint32_t GetReceiverThreadCoreId() {
    return GetInstance().gReceiverThreadCoreId;
  }

  static uint32_t GetEcThreadCoreId() {
    return GetInstance().gEcThreadCoreId;
  }

  static uint32_t GetIndexThreadCoreId() {
    return GetInstance().gIndexThreadCoreId;
  }

  static uint32_t GetDispatchThreadCoreId() {
    return GetInstance().gDispatchThreadCoreId;
  }

  static uint32_t GetCompletionThreadCoreId() {
    return GetInstance().gCompletionThreadCoreId;
  }

  static uint32_t GetIoThreadCoreId(uint32_t thread_id) {
    return GetInstance().gIoThreadCoreIdBase + thread_id;
  }

  static void SetStorageSpaceInBytes(uint64_t storageSpaceInBytes) {
    GetInstance().gStorageSpaceInBytes = storageSpaceInBytes;
  }

  static uint64_t GetStorageSpaceInBytes() {
    return GetInstance().gStorageSpaceInBytes;
  }

  static void SetL2PTableSizeInBytes(uint64_t bytes) {
    GetInstance().gL2PTableSize = bytes;
  }

  static uint64_t GetL2PTableSizeInBytes() {
    return GetInstance().gL2PTableSize;
  }

  static void SetEnableEventFramework(bool enable) {
    GetInstance().gEnableEventFramework = enable;
  }

  static bool GetEventFrameworkEnabled() {
    return GetInstance().gEnableEventFramework;
  }

  static void SetEnableRedirection(bool enable) {
    GetInstance().gEnableRedirection = enable;
  }

  static bool GetRedirectionEnable() {
    return GetInstance().gEnableRedirection;
  }

  static void SetInjectCrash() {
    GetInstance().gInjectCrash = true;
  }

  static bool InjectCrash() {
    return GetInstance().gInjectCrash;
  }

  // debug
  static bool DebugMode() {
    return GetInstance().gDebugMode;
  }

  static bool DebugMode2() {
    return GetInstance().gDebugMode2;
  }

  static void SetDebugMode() {
    GetInstance().gDebugMode = true;
  }

  static void SetDebugMode2() {
    GetInstance().gDebugMode2 = true;
  }

  static bool StuckDebugMode() {
    return GetInstance().gStuckDebugMode;
  }

  static void SetStuckDebugMode(double timeNow) {
    if (GetInstance().gStuckDebugMode == false) {
      fprintf(stderr, "Stuck debug mode triggered. Possible reason %s\n",
          GetInstance().gPossibleStuckReason.c_str());
    }
    GetInstance().gStuckDebugMode = true;
    GetInstance().gStuckDebugModeTime = timeNow;
  }

  static void SetPossibleStuckReason(std::string s) {
    GetInstance().gPossibleStuckReason = s;
  }

  static bool StuckDebugModeFinished(double timeNow) {
    return timeNow - GetInstance().gStuckDebugModeTime > 1.0;
  }

  static void IncreaseStuckCounter() {
    GetInstance().stuckCounter++;
  }

  static uint32_t GetStuckCounter() {
    return GetInstance().stuckCounter;
  }

  static std::string DebugIndexMappingFile() {
//    return "/home/jhli/git_repos/zapraid_extension/zapraid_prototype/build/debug_file/index_mapping.txt";
    return "/mnt/data/debug_file/index_mapping.txt";
  }

  static uint32_t CalculateDiskId(uint32_t stripeId, uint32_t whichBlock, RAIDLevel raidScheme, uint32_t numDisks) {
    // calculate which disk current block (data/parity) should go
    uint32_t driveId = ~0u;
    uint32_t idOfGroup = stripeId % numDisks;

    if (raidScheme == RAID0
        || raidScheme == RAID1
        || raidScheme == RAID01
        || raidScheme == RAID4) {
      driveId = whichBlock;
    } else if (raidScheme == RAID5) {
      // Example: 0 1 P
      //          0 P 1
      //          P 0 1
      if (whichBlock == numDisks - 1) { // parity block
        driveId = whichBlock - idOfGroup;
      } else if (whichBlock + idOfGroup >= numDisks - 1) {
        driveId = whichBlock + 1;
      } else {
        driveId = whichBlock;
      }
    } else if (raidScheme == RAID6 && numDisks == 4) {
      driveId = raid6_4drive_mapping[stripeId % numDisks][whichBlock];
    } else if (raidScheme == RAID6 && numDisks == 5) {
      // A1 A2 A3 P1 P2
      // B1 B2 P1 P2 B3
      // C1 P1 P2 C2 C3
      // P1 P2 D1 D2 D3
      // P2 E1 E2 E3 P1
      // ...
      driveId = raid6_5drive_mapping[stripeId % numDisks][whichBlock];
    }
    else if (raidScheme == RAID6 && numDisks == 6)
    {
      driveId = raid6_6drive_mapping[stripeId % numDisks][whichBlock];
    }
    else
    {
      fprintf(stderr, "RAID scheme not supported!\n");
    }
    return driveId;
  }

  static uint32_t GetStripeIdFromOffset(uint32_t unitSize, uint32_t offset) 
  {
    uint32_t blockSize = GetBlockSize();
    if (offset == 0) {
      return 0;
    } else {
      return (offset - 1) / (unitSize / blockSize) + 1;
    }
  }


private:
  uint32_t gRebootMode = 0; // 0: new, 1: restart, 2: rebuild.

  StripeConfig* gStripeConfig = new StripeConfig[1];
  int gBlockSize = 4096;
  int gMetadataSize = 64;
  int gNumIoThreads = 1;
  bool gDeviceSupportMetadata = true;
  int gZoneCapacity = 0;
  int gStripePersistencyMode = 0;
  bool gEnableGc = true;
  bool gEnableDegradedRead = false;
  bool gEnableIOLatencyTest = true;
  uint32_t gNumOpenSegments = 1;
  RAIDLevel gRaidScheme = RAID5;
  bool gEnableHeaderFooter = true;
  bool gEnableRedirection = false;
  bool gInjectCrash = false;

  uint64_t gStorageSpaceInBytes = 1024 * 1024 * 1024 * 1024ull; // 1TiB
  uint64_t gL2PTableSize = 0; // Infinity 

  SystemMode gSystemMode = ZAPRAID;

  uint32_t gReceiverThreadCoreId = 3;
  uint32_t gDispatchThreadCoreId = 4;
  // Not used for now; functions collocated with dispatch thread.
  uint32_t gCompletionThreadCoreId = 5;
  uint32_t gIndexThreadCoreId = 6;
  uint32_t gEcThreadCoreId = 7;
  uint32_t gIoThreadCoreIdBase = 8;

  int gLargeRequestThreshold = 16 * 1024;

  // SPDK target adopts the reactors framework, and we should not manually initiate any SPDK thread
  bool gEnableEventFramework = false;

  // debug
  bool gDebugMode = false;
  bool gDebugMode2 = false;
  bool gStuckDebugMode = false;
  double gStuckDebugModeTime = 0.0;
  std::string gPossibleStuckReason = "";
  uint32_t stuckCounter = 0;
};


#endif
