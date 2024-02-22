#ifndef __INDEXMAP_H__
#define __INDEXMAP_H__

#include "common.h"
#include <set>

// notes: when you modify content, check whether mBlockIndices is updated.

struct MappingBlock {
  uint32_t* content = nullptr; // 4096B array
  uint32_t pba = ~0u;
  bool dirty = false;
  bool fetching = false;
  bool prepareFetching = false;
  bool evicting = false;
};

enum IndexMapStatus {
  kHit = 0,
  kNeedEvict = 1,
  kNeedRead = 2,
  kEvicting = 3,
  kFetching = 4,
  kNormal = 5,
  kReadFailed = 6,
  kReadSuccess = 7,
  kEvictFailed = 8,
  kEvictSuccess = 9,
  // for updating mapping blocks during GC
  kUpdatedSuccess = 10,
  kUpdatedFailedForDirty = 11,
  kUpdatedFailedForInvalid = 12,
};

class IndexMap {
public:
  /**
   * @brief  
   *  
   * @param capacity in blocks 
   * @param memory the number of in-memory blocks
   */
  IndexMap(uint64_t capacity, uint64_t memory);
  ~IndexMap();

  /**
   * @brief  
   *  
   * @param lba in blocks
   * @param pba in blocks 
   * @return true if update successfully, false if need to evict or read 
   */
  IndexMapStatus UpdatePba(uint64_t lba, uint32_t pba, uint32_t& oldMap);

  /**
   * @brief  
   *  
   * @param lba in blocks
   * @param pba in blocks 
   * @return 
   */
  IndexMapStatus GetPba(uint64_t lba, uint32_t& pba);
  IndexMapStatus HandleContext(RequestContext* ctx, uint32_t& oldMapPba);
  void FillContext(RequestContext* ctx, IndexMapStatus s, uint64_t lba);

  IndexMapStatus UpdateMappingBlockPba(uint64_t lba, uint32_t oldPba,
      uint32_t newPba);

  bool IsAllInMemory(); 
  inline uint32_t GetSizeUnit() {
    return mSizeUnit;
  }

private:
  MappingBlock* mBlocks = nullptr;
  uint64_t mNumBlocks = 0;
  uint64_t mMemBlocks = 0;
  uint64_t mSizeUnit = 0;
  uint64_t mFirstEmpty = 0;

  // cache
  uint8_t* mMemory = 0;
  uint32_t* mBlockIndices = nullptr; // reverse mapping
  uint8_t* mClockBits = 0;
  uint32_t mClockIt = 0;

  std::set<uint32_t> emptyLines;
  std::set<uint32_t> evictingLines;
  std::set<uint32_t> fetchingLines; 

  uint32_t mEvictingThreshold = 16; // so that it can fill a whole stripe under 16KiB blocks
  IndexMapStatus mEvictStatus = kNormal, mFetchStatus = kNormal;
  uint32_t PrepareReadBlock(uint32_t blockIndex);
  uint32_t Evict(bool& needSwap);
};

#endif

