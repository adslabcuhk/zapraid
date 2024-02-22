#include "indexmap.h"
#include "common.h"
#include "configuration.h"

IndexMap::IndexMap(uint64_t capacity, uint64_t memory) {
  assert(capacity <= 4ull * 1024 * 1024 * 256); // at most 4TiB (4K blocks) 
  printf("IndexMap: capacity %lu, memory %lu\n", capacity, memory);

  // a mapping block for each 4MiB data
  uint32_t blockSize = Configuration::GetBlockSize();
  uint64_t sizeUnit = blockSize / 4;  // the number of mappings in a block
  mSizeUnit = sizeUnit;
  mNumBlocks = round_up(capacity, mSizeUnit) / mSizeUnit;
  mBlocks = new MappingBlock[mNumBlocks];
  if (memory == 0) {
    // full mapping, do not need to dump to the ssd
    mMemBlocks = mNumBlocks;
//    for (int i = 0; i < mNumBlocks; i++) {
//      mBlocks[i].content = (uint32_t*)(mMemory + i * blockSize);
//    }
  } else {
    mMemBlocks = memory;
  }
  mMemory = new uint8_t[mMemBlocks * blockSize];
  memset(mMemory, 0xff, mMemBlocks * blockSize);
  mBlockIndices = new uint32_t[mMemBlocks];
  memset(mBlockIndices, 0xff, mMemBlocks * sizeof(uint32_t));
  mClockBits = new uint8_t[(mMemBlocks + 7) / 8];
  memset(mClockBits, 0, (mMemBlocks + 7) / 8);
}

IndexMap::~IndexMap() {
  if (mMemory) {
    delete[] mMemory;
  }
  if (mBlocks) {
    delete[] mBlocks;
  }
}

IndexMapStatus IndexMap::UpdatePba(uint64_t lba, uint32_t pba, 
    uint32_t& oldMapPba) {
  static uint32_t printFirstEmpty = 8192;
  uint32_t blockSize = Configuration::GetBlockSize();
  assert(lba < mNumBlocks * mSizeUnit);
  assert(pba < 4ull * 1024 * 1024 * 1024); // at most 16TiB (4K blocks)

  uint32_t blockIndex = lba / mSizeUnit;
  uint32_t offset = lba % mSizeUnit;
  uint32_t*& content = mBlocks[blockIndex].content;
  uint32_t& blockPba = mBlocks[blockIndex].pba;
  oldMapPba = ~0u; // no need to invalidate a mapping block

  if (content == nullptr) {
    if (blockPba == ~0u) {
      // compulsory miss. Not written before
      if (mFirstEmpty < mMemBlocks) {
        // the cache is not full, and write for the first time
        content = (uint32_t*)(mMemory + mFirstEmpty * blockSize);
        // no need to clean the content.
        content[offset] = pba;
        mBlockIndices[mFirstEmpty] = blockIndex;
        mBlocks[blockIndex].dirty = true;
        mFirstEmpty++;
        if (mFirstEmpty >= printFirstEmpty || mFirstEmpty == mMemBlocks) {
          debug_error("first empty: %lu\n", mFirstEmpty);
          printFirstEmpty *= 2;
        }
        return kHit;
      } else if (!emptyLines.empty()) {
        // the cache is evicted before, can use empty lines to write
        uint32_t firstEmpty = *emptyLines.begin();
        emptyLines.erase(firstEmpty);
        content = (uint32_t*)(mMemory + firstEmpty * blockSize);
        // clean the content because it may be the previous content
        memset(content, 0xff, blockSize);
        content[offset] = pba;
        mBlockIndices[firstEmpty] = blockIndex;
        mBlocks[blockIndex].dirty = true;
        return kHit;
      } else {
        // the cache is full. Need to evict
        return kNeedEvict;
      }
    } else {
      // written before
      if (!emptyLines.empty()) {
        // occupy the memory block first
        PrepareReadBlock(blockIndex);
        // directly read the block and do not need to evict
        // let the caller prepare the context and read
        return kNeedRead;
      } else {
        return kNeedEvict;
      }
    }
  } else {
    // CLOCK: set the clock bit to 1
    uint32_t memOffset = ((uint8_t*)content - mMemory) / blockSize;
    mClockBits[memOffset / 8] |= (1 << (memOffset % 8));
    // mapped
    content[offset] = pba;
    mBlocks[blockIndex].dirty = true;
    oldMapPba = blockPba;
    blockPba = ~0u;
    return kHit;
  }
}

// lba is LBA in blocks
IndexMapStatus IndexMap::GetPba(uint64_t lba, uint32_t& pba) {
//  uint32_t blockSize = Configuration::GetBlockSize();
  assert(lba < mNumBlocks * mSizeUnit);

  uint32_t blockIndex = lba / mSizeUnit;
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t offset = lba % mSizeUnit;
  uint32_t*& content = mBlocks[blockIndex].content;
  uint32_t& blkPba = mBlocks[blockIndex].pba;
  bool& fetching = mBlocks[blockIndex].fetching;

  if (content == nullptr) {
    // it means that the cache needs to fetch a block 
    if (blkPba == ~0u) {
      // the block is invalid and not written yet
      // no need to be in the memory
      pba = ~0u;
      return kHit;
    } 
    
    assert(mFirstEmpty == mMemBlocks); 
    if (!emptyLines.empty()) {
      PrepareReadBlock(blockIndex);
      return kNeedRead;
    } else {
      return kNeedEvict;
    }
  } else {
    // fetching: meaning that the content is still invalid.
    if (fetching) {
      return kFetching;
    }
    // evicting is ok; the content is valid
    // CLOCK: set the clock bit to 1
    uint32_t memOffset = ((uint8_t*)content - mMemory) / blockSize;
    mClockBits[memOffset / 8] |= (1 << (memOffset % 8));
    if (content[offset] == ~0u) {
      pba = ~0u;
    } else {
      pba = content[offset];
    }
    return kHit;
  }
}

// evict using the CLOCK algorithm
// return the evicted block index
uint32_t IndexMap::Evict(bool& needSwap) {
  uint32_t cnt = 0;
  needSwap = false;
  while (cnt < mMemBlocks * 2) {
    // the number of iterations. If scanned for twice but no block is
    // evicted, return a false message
    cnt++;
    mClockIt = (mClockIt + 1) % mMemBlocks;

    if ((mClockBits[mClockIt / 8] & (1 << (mClockIt % 8))) == 0) {
      uint32_t blockIndex = mBlockIndices[mClockIt];
      uint32_t blockSize = Configuration::GetBlockSize();
      if (blockIndex == ~0u) continue;
      MappingBlock& blk = mBlocks[blockIndex];
      if (blk.evicting) continue;
      assert(blk.fetching == false);

      // evict
      if (!blk.dirty) {
        // directly remove the block 
        blk.content = nullptr;
        mBlockIndices[mClockIt] = ~0u;
        emptyLines.insert(mClockIt);
        needSwap = false;
        return blockIndex;
      } else {
        // evict the dirty block
        blk.evicting = true;
        needSwap = true;
        return blockIndex;
      }
    } else {
      // set the bit to 0
      mClockBits[mClockIt / 8] &= ~(1 << (mClockIt % 8));
    }
  }

  return ~0u;
}

// 1st phase: set fetching bit (this function)
// 2nd phase: insert into fetchingLines (fill context function)
// 3rd phase: (after I/O) reset fetching bit and remove fetchingLines
uint32_t IndexMap::PrepareReadBlock(uint32_t blockIndex) {
  auto& blk = mBlocks[blockIndex];
  // fetching can be true if the evict is for this block
  assert(!blk.fetching && !blk.evicting);
  assert(blk.evicting == false);
  assert(blk.content == nullptr);
  assert(blk.pba != ~0u);
  assert(emptyLines.size() > 0);

  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t firstEmpty = *emptyLines.begin();
  
  blk.content = (uint32_t*)(mMemory + firstEmpty * blockSize);
  mBlockIndices[firstEmpty] = blockIndex;
  emptyLines.erase(firstEmpty);
  blk.fetching = true;
  return blockIndex;
}

// lba is the LBA for the data (in bytes) 
// should be called after a kNeedEvict or kNeedRead is returned. to make sure
// that evictingLines or fetchingLines is not empty.
void IndexMap::FillContext(RequestContext* ctx, IndexMapStatus lastStatus, uint64_t lba) {
  static int stuckedPrint = 0;
  assert(!ctx->available);
  ctx->type = INDEX;
  ctx->associatedRequests.clear();
  ctx->cb_fn = nullptr;
  ctx->cb_args = nullptr;
  uint32_t blockSize = Configuration::GetBlockSize();

  if (lastStatus == kNeedEvict) {
    bool needSwap;
    // check whether too many lines are evicting 
    if (evictingLines.size() >= mEvictingThreshold) {
      // directly return
      if (Configuration::StuckDebugMode() && stuckedPrint == 0) {
        stuckedPrint = 1;
        printf("%s %d: evicting lines %lu\n", __func__, __LINE__,
            evictingLines.size());
        for (auto& it : evictingLines) {
          printf("%u (index %u lba %lu)\n", it, mBlockIndices[it], 
              mBlockIndices[it] * mSizeUnit * blockSize);
        }
      }
      ctx->status = WRITE_INDEX_WAITING;
      return;
    }

    uint32_t blockIndex = Evict(needSwap);
    assert(blockIndex != ~0u);

    if (needSwap) {
      ctx->req_type = 'W';
      ctx->status = WRITE_INDEX_REAPING; 
      memcpy(ctx->data, mBlocks[blockIndex].content, blockSize);
      mBlocks[blockIndex].dirty = false;
      ctx->lba = blockIndex * mSizeUnit * blockSize;
      ctx->size = blockSize;
      ctx->targetBytes = blockSize;
      ctx->successBytes = 0;
      if (mClockIt != ((uint8_t*)(mBlocks[blockIndex].content) - mMemory) / blockSize) {
        debug_error("clockIt %u, blockIndex %u, memIt %lu\n", mClockIt, blockIndex,
            ((uint8_t*)(mBlocks[blockIndex].content) - mMemory) / blockSize);
        assert(0);
      }
      evictingLines.insert(mClockIt);
    } else {
      // directly deleted the block. Don't need to update
      ctx->status = WRITE_INDEX_COMPLETE;
      ctx->lba = ~0ull;
      ctx->size = ctx->targetBytes = 0;
    }
  } else if (lastStatus == kNeedRead) {
    uint32_t blockIndex = lba / mSizeUnit / blockSize;
    if (mBlocks[blockIndex].content == nullptr) {
      // may be nullptr if previously the evicted block is directly removed
      PrepareReadBlock(blockIndex);
    }
    ctx->data = (uint8_t*)mBlocks[blockIndex].content;
    ctx->req_type = 'R';
    ctx->status = READ_INDEX_REAPING;
    ctx->lba = lba; // original LBA for reading. It's ok 
    assert(mBlocks[blockIndex].pba != 0);  // 0 is in the header
    ctx->pbaInBlk = mBlocks[blockIndex].pba;
    // need RAIDController to transfer it to pbaArray
    ctx->offset = 0;
    ctx->size = blockSize;
    ctx->targetBytes = blockSize;
    fetchingLines.insert(
        ((uint8_t*)(mBlocks[blockIndex].content) - mMemory) / blockSize);
  }
}

IndexMapStatus IndexMap::HandleContext(RequestContext* ctx, uint32_t& oldPba) {
  // reads: remember to put the updates into the content 
  // writes: remember to abort the evict if the block becomes dirty again
  IndexMapStatus s;
  uint32_t blockSize = Configuration::GetBlockSize();
  assert(ctx->type == INDEX);

  uint32_t blockIndex = ctx->lba / mSizeUnit / blockSize;
  assert(blockIndex < mNumBlocks);
  auto& blk = mBlocks[blockIndex];

  if (ctx->status == READ_INDEX_COMPLETE) {
    // read complete, can read the index again 
    assert(blk.fetching);
    assert(!blk.evicting);
    assert(!blk.dirty);
    if (blk.pba != ctx->pbaInBlk) { // the block is rewritten by GC
//      ctx->printMsg();
//      printf("pba pbaInBlk %u %u\n", blk.pba, ctx->pbaInBlk);
//      if (mMessages.count(blockIndex)) {
//        mMessages[blockIndex]->printMsg();
//      }
//      assert(0);
      // release the block, and the index thread will read the index again
      blk.fetching = false;
      uint32_t memIt = ((uint8_t*)blk.content - mMemory) / blockSize;
      blk.content = nullptr;
      fetchingLines.erase(memIt);
      emptyLines.insert(memIt);
      mBlockIndices[memIt] = ~0u;
      s = kReadFailed;
    } else {
      blk.fetching = false;
      uint32_t memIt = ((uint8_t*)blk.content - mMemory) / blockSize;
      fetchingLines.erase(memIt);
      mClockBits[memIt / 8] |= (1 << (memIt % 8));
      memcpy(blk.content, ctx->data, blockSize); 

      // do not need to update pba
      // handle associated context: can be 
      s = kReadSuccess;
    }
  } else if (ctx->status == WRITE_INDEX_COMPLETE) {
    assert(blk.evicting && !blk.fetching);

    if (blk.dirty) { // the block is written during eviction
      blk.evicting = false;
      uint32_t memIt = ((uint8_t*)blk.content - mMemory) / blockSize;
      assert(evictingLines.find(memIt) != evictingLines.end());
      evictingLines.erase(memIt);
      s = kEvictFailed;
    } else { // eviction succeed.
      blk.evicting = false;
      uint32_t memIt = ((uint8_t*)blk.content - mMemory) / blockSize;
      assert(evictingLines.find(memIt) != evictingLines.end());
      evictingLines.erase(memIt);
      emptyLines.insert(memIt);
      blk.content = nullptr;
      mBlockIndices[memIt] = ~0u;
      mClockBits[memIt / 8] |= (1 << (memIt % 8));
      oldPba = blk.pba;
      blk.pba = ctx->pbaInBlk;
      s = kEvictSuccess;
    }
  }
  return s;
}

IndexMapStatus IndexMap::UpdateMappingBlockPba(uint64_t lba,
    uint32_t oldPbaInBlk, uint32_t newPbaInBlk) {
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t blockIndex = lba / mSizeUnit / blockSize;
  assert(blockIndex < mNumBlocks);
  auto& blk = mBlocks[blockIndex];
  if (blk.pba == oldPbaInBlk) {
    if (blk.dirty) {
      // a dirty should not have a pba
      assert(0);
    } else {
      blk.pba = newPbaInBlk;
      return kUpdatedSuccess;
    }
  } else {
    return kUpdatedFailedForInvalid;
  }
}

bool IndexMap::IsAllInMemory() {
  return mMemBlocks >= mNumBlocks;
}


