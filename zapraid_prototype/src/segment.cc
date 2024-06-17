#include "segment.h"

#include <sys/time.h>
#include "raid_controller.h"

Segment::Segment(RAIDController *raidController,
                 uint32_t segmentId,
                 RequestContextPool *ctxPool,
                 ReadContextPool *rctxPool,
                 StripeWriteContextPool *sctxPool, 
                 uint32_t configurationId)
{
  int stripeSize = Configuration::GetStripeSize(configurationId);
  int stripeUnitSize = Configuration::GetStripeUnitSize(configurationId);
  int stripeDataSize = Configuration::GetStripeDataSize(configurationId);
  int n = stripeSize / stripeUnitSize;
  int k = stripeDataSize / stripeUnitSize;
  uint32_t stripeGroupSize = Configuration::GetStripeGroupSize(configurationId);
  uint8_t useAppend = stripeGroupSize > 1;

  initSegment(raidController, ctxPool, rctxPool, sctxPool, segmentId,
      stripeSize, n, k, useAppend, stripeGroupSize);
}

// used for recovery, using existing metadata
Segment::Segment(RAIDController *raidController, 
                 RequestContextPool *ctxPool,
                 ReadContextPool *rctxPool,
                 StripeWriteContextPool *sctxPool, 
                 SegmentMetadata* segMeta)
{
//  if (segmentId != segMeta->segmentId) {
//    debug_warn("segmentId found %d but not %d\n", segmentId,
//        segMeta->segmentId); 
//  }
  uint32_t segmentId = segMeta->segmentId;
  int stripeSize = segMeta->stripeSize;
  int n = segMeta->n;
  int k = segMeta->k;
  uint8_t useAppend = segMeta->useAppend;
  uint32_t stripeGroupSize = segMeta->stripeGroupSize;

  initSegment(raidController, ctxPool, rctxPool, sctxPool, segmentId,
      stripeSize, n, k, useAppend, stripeGroupSize);
}

void Segment::initSegment(RAIDController *raidController, 
                 RequestContextPool *ctxPool,
                 ReadContextPool *rctxPool,
                 StripeWriteContextPool *sctxPool, uint32_t segmentId,
                 int stripeSize, int n, int k, uint8_t useAppend,
                 uint32_t stripeGroupSize) {
  int blockSize = Configuration::GetBlockSize();
  mRaidController = raidController;
  mPos = 0;
  mPosInStripe = 0;
  mCurStripeId = 0;

  mRequestContextPool = ctxPool;
  mReadContextPool = rctxPool;
  mStripeWriteContextPool = sctxPool;

  mAdminStripe = new StripeWriteContext();
  mAdminStripe->data = new uint8_t *[stripeSize / blockSize];
  mAdminStripe->metadata = new uint8_t *[stripeSize / blockSize];

  SystemMode mode = Configuration::GetSystemMode();

  // Initialize segment metadata
  mSegmentMeta.segmentId = segmentId;
  mSegmentMeta.stripeSize = stripeSize;
  mSegmentMeta.stripeDataSize = stripeSize / n * k;
  mSegmentMeta.stripeParitySize = stripeSize / n * (n-k);
  mSegmentMeta.stripeGroupSize = stripeGroupSize;
  mSegmentMeta.stripeUnitSize = stripeSize / n;
  mSegmentMeta.n = n;
  mSegmentMeta.k = k;
  mSegmentMeta.raidScheme = Configuration::GetRaidLevel();
  mSegmentMeta.numZones = 0;
  mSegmentMeta.useAppend = (mode == ZONEWRITE_ONLY || mode == RAIZN_SIMPLE || (mode == ZAPRAID && !useAppend)) ? false : true; 

  mNumBlocks = 0;
  mNumInvalidBlocks = 0;
  mNumIndexBlocks = 0;

  mHeaderRegionSize = raidController->GetHeaderRegionSize();
    //mSegmentMeta.stripeUnitSize / Configuration::GetBlockSize()
  mDataRegionSize = raidController->GetDataRegionSize();
  mFooterRegionSize = raidController->GetFooterRegionSize();

  uint32_t dataRegionStripes = mDataRegionSize / (mSegmentMeta.stripeUnitSize /
      blockSize);

  mvblength = round_up(mSegmentMeta.n * mDataRegionSize, 8);
  mValidBits = new uint8_t[mvblength];
  memset(mValidBits, 0, mvblength);

  // TODO Remove compact stripe table for Zone Write

  if (mSegmentMeta.useAppend) {
    if (mode == ZONEAPPEND_ONLY) {
      // the largest group size for ZoneAppend-Only
      mSegmentMeta.stripeGroupSize = dataRegionStripes;
    }
    if (mSegmentMeta.stripeGroupSize <= 256) { // less than 8 bits
      mCompactStripeTable = new uint8_t[mSegmentMeta.n * dataRegionStripes];
      memset(mCompactStripeTable, 0, mSegmentMeta.n * dataRegionStripes);
    } else if (mSegmentMeta.stripeGroupSize <= 65536) { // 16 bits
      mCompactStripeTable = new uint8_t[2 * mSegmentMeta.n * dataRegionStripes];
      memset(mCompactStripeTable, 0, 2 * mSegmentMeta.n * dataRegionStripes);
    } else {
      mCompactStripeTable = new uint8_t[3 * mSegmentMeta.n * dataRegionStripes];
      memset(mCompactStripeTable, 0, 3 * mSegmentMeta.n * dataRegionStripes);
    }
  }

  if (Configuration::GetSystemMode() == RAIZN_SIMPLE) {
    mMetaZoneList.resize(n);
  }

  mCodedBlockMetadata = new CodedBlockMetadata[mSegmentMeta.n * mDataRegionSize];
  memset(mCodedBlockMetadata, 0, mSegmentMeta.n * mDataRegionSize *
      sizeof(CodedBlockMetadata));

  mSegmentStatus = SEGMENT_NORMAL;

  mLastStripeCreationTimestamp = GetTimestampInUs();
  mLastAppendTimestamp = mLastStripeCreationTimestamp;
  gettimeofday(&mTimevalStripe, 0);
}

Segment::~Segment()
{
  // TODO: reclaim zones to devices
  delete []mValidBits;
  if (mCompactStripeTable) {
    delete []mCompactStripeTable;
  }
  if (mCodedBlockMetadata) {
    delete []mCodedBlockMetadata;
  }
}

void Segment::recycleStripeWriteContexts()
{
  mStripeWriteContextPool->Recycle();
}

void Segment::AddZone(Zone *zone)
{
  mSegmentMeta.zones[mZones.size()] = zone->GetSlba();
  mSegmentMeta.numZones += 1;
  mZones.emplace_back(zone);
}

// must be called in the dispatch thread
void Segment::PushBackMetaZone(Zone *zone, int devId) {
  if (Configuration::GetSystemMode() != RAIZN_SIMPLE) {
    assert(0);
  }
  assert(devId < mMetaZoneList.size());
  mMetaZoneList[devId].push_back(zone);
  debug_warn("list dev %d size %lu zone %p \n", devId, mMetaZoneList[devId].size(), zone);
}

// must be called in the dispatch thread
uint32_t Segment::PopFrontMetaZone(Zone *zone, int devId) {
  if (Configuration::GetSystemMode() != RAIZN_SIMPLE) {
    assert(0);
  }
  assert(devId < mMetaZoneList.size());
  assert(!mMetaZoneList[devId].empty());
  Zone* z = mMetaZoneList[devId].front();
  if (z == zone) {
    mMetaZoneList[devId].pop_front();
  }
//  if (devId == 0) {
//    debug_error("list dev %d size %lu zone Pos %u\n", devId,
//        mMetaZoneList[devId].size(), zone->GetIssuedPos());
//  }
  return (z == zone ? 1 : 0);
}

void Segment::CleanStripeContext(StripeWriteContext *stripe) {
  for (auto slot : stripe->ioContext) {
    mRequestContextPool->ReturnRequestContext(slot);
  }
  stripe->ioContext.clear();
}

const std::vector<Zone*>& Segment::GetZones()
{
  return mZones;
}

uint32_t Segment::GetNumBlocks() const
{
  return mNumBlocks;
}

uint32_t Segment::GetNumInvalidBlocks() const
{
  return mNumInvalidBlocks;
}

uint32_t Segment::GetFullNumBlocks() const
{
  return mSegmentMeta.k * mDataRegionSize; 
}

bool Segment::IsFull()
{
  return mPos == mHeaderRegionSize + mDataRegionSize;
}

bool Segment::CanSeal()
{
  return mPos >= mHeaderRegionSize + mDataRegionSize + mFooterRegionSize;
}

uint32_t Segment::GetStripeIdFromOffset(uint32_t offset) 
{
  return offset / (mSegmentMeta.stripeUnitSize / Configuration::GetBlockSize());
//  uint32_t unitSize = mSegmentMeta.stripeUnitSize;
//  uint32_t blockSize = Configuration::GetBlockSize();
//  if (offset < mHeaderRegionSize) {
//    return 0;
//  } else {
//    return (offset - mHeaderRegionSize) / (unitSize / blockSize) + 1;
//  }
}

void Segment::PrintStats()
{
  printf("Zone group position: %d, capacity: %d, num invalid blocks: %d\n", mPos, mDataRegionSize, mNumInvalidBlocks);
  for (auto zone : mZones) {
    zone->PrintStats();
  }
}

bool Segment::checkStripeAvailable(StripeWriteContext *stripe)
{
  bool isAvailable = true;

  for (auto slot : stripe->ioContext) {
    isAvailable = slot && slot->available ? isAvailable : false;
//    debug_warn("slot %p available %d\n", slot, slot->available);
  }

  return isAvailable;
}

SegmentStatus Segment::GetStatus()
{
  return mSegmentStatus;
}

void progressFooterWriter2(void *arg1, void *arg2) {
 Segment *segment = reinterpret_cast<Segment*>(arg1);
 segment->ProgressFooterWriter();
}

void progressFooterWriter(void *args) {
  progressFooterWriter2(args, nullptr);
}

// StateTransition must be called in the same thread
// as Append()
// dispatch thread
bool Segment::StateTransition()
{
  double timestamp = GetTimestampInUs();
  bool stateChanged = false;
  if (mSegmentStatus == SEGMENT_NORMAL) {
    if (mPos == mHeaderRegionSize + mDataRegionSize) {
      mSegmentStatus = SEGMENT_PREPARE_FOOTER;
    } else {
//      if (!Configuration::InjectCrash() && timestamp - mLastStripeCreationTimestamp >= 
//          StatsRecorder::getInstance()->getP999WriteLatInUs() / 1000000.0) 
      if (!Configuration::InjectCrash() && 
          Configuration::GetSystemMode() != RAIZN_SIMPLE && 
          timestamp - mLastAppendTimestamp >= getSyncTimeoutInUs() / 1000000.0) 
      {
        // fill and flush the last stripe if no further write request for 1ms interval
        FlushCurrentStripe();
      }
    }
  }

  uint32_t blockSize = Configuration::GetBlockSize();

  if (mSegmentStatus == SEGMENT_CONCLUDING_APPENDS_IN_GROUP
      || mSegmentStatus == SEGMENT_CONCLUDING_WRITES_IN_GROUP) {
    mStripeWriteContextPool->Recycle();
    if (mStripeWriteContextPool->NoInflightStripes()) {
//      if (mSegmentMeta.stripeGroupSize > 1) {
//        printf("segment id %lu back to normal mPos %u\n", mSegmentMeta.segmentId, mPos);
//      }
      mSegmentStatus = SEGMENT_NORMAL;
      stateChanged = true;
    }
  } else if (mSegmentStatus == SEGMENT_WRITING_HEADER) {
    // wait for finalizing the header
    if (checkStripeAvailable(mCurStripe) && mStripeWriteContextPool->NoInflightStripes()) {
      for (auto slot : mCurStripe->ioContext) {
        mRequestContextPool->ReturnRequestContext(slot);
      }
      mCurStripe->ioContext.clear();
      stateChanged = true;
      mSegmentStatus = SEGMENT_NORMAL;
    }
  } else if (mSegmentStatus == SEGMENT_PREPARE_FOOTER) {
    // wait for persisting the stripes
    mStripeWriteContextPool->Recycle();
    if (mStripeWriteContextPool->NoInflightStripes()) {
      // prepare the stripe for writing the footer
      mCurStripe = mAdminStripe;
      mCurStripe->targetBytes = mSegmentMeta.n * mSegmentMeta.stripeUnitSize;
      mCurStripe->totalTargetBytes = mSegmentMeta.n * mSegmentMeta.stripeUnitSize;
        //mSegmentMeta.stripeSize;  // TODO change to chunk size 
      for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
        RequestContext *slot = mRequestContextPool->GetRequestContext(true);
        slot->data = slot->dataBuffer;
        slot->meta = slot->metadataBuffer;
        slot->targetBytes = mSegmentMeta.stripeUnitSize;
        slot->type = STRIPE_UNIT;
        slot->size = mSegmentMeta.stripeUnitSize; 
        slot->segment = this;
        slot->ctrl = mRaidController;
        slot->lba = ~0ull;
        slot->associatedRequests.clear(); // = nullptr;
        slot->associatedStripe = mCurStripe;
        slot->available = true;
        slot->append = false;

        mCurStripe->data[i] = (uint8_t*)slot->data;
        mCurStripe->ioContext.emplace_back(slot);

        mSegmentStatus = SEGMENT_WRITING_FOOTER;
      }
      stateChanged = true;
    }
  } else if (mSegmentStatus == SEGMENT_WRITING_FOOTER) {
    if (checkStripeAvailable(mCurStripe)) {
      if (CanSeal()) {
        stateChanged = true;
        mSegmentStatus = SEGMENT_SEALING;
        Seal();
      } else {
        for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
          uint32_t zoneId = Configuration::CalculateDiskId(
            mCurStripeId, i, mSegmentMeta.raidScheme, mSegmentMeta.numZones
             // mPos, i, mSegmentMeta.raidScheme, mSegmentMeta.numZones
              );
          RequestContext *slot = mCurStripe->ioContext[i];
          slot->available = false;
        }
        if (!Configuration::GetEventFrameworkEnabled()) {
          thread_send_msg(mRaidController->GetEcThread(), progressFooterWriter, this);
        } else {
          event_call(Configuration::GetEcThreadCoreId(), progressFooterWriter2, this, nullptr);
        }
      }
    }
  } else if (mSegmentStatus == SEGMENT_SEALING) {
    if (checkStripeAvailable(mCurStripe)) {
      for (auto slot : mCurStripe->ioContext) {
        mRequestContextPool->ReturnRequestContext(slot);
      }
      mCurStripe->ioContext.clear();
      stateChanged = true;
      delete []mAdminStripe->data;
      delete []mAdminStripe->metadata;
      delete mAdminStripe;
      if (Configuration::GetDeviceSupportMetadata()) {
        delete []mCodedBlockMetadata;
        mCodedBlockMetadata = nullptr;
      }
      mSegmentStatus = SEGMENT_SEALED;
    }
  }

  return stateChanged;
}

void finalizeSegmentHeader2(void *arg1, void *arg2)
{
  Segment *segment = reinterpret_cast<Segment*>(arg1);
  segment->FinalizeSegmentHeader();
}

void finalizeSegmentHeader(void *args)
{
  finalizeSegmentHeader2(args, nullptr);
}

void Segment::FinalizeCreation()
{
  mSegmentStatus = SEGMENT_WRITING_HEADER;
  StripeWriteContext *stripe = mAdminStripe;
  mCurStripe = stripe;
  stripe->ioContext.resize(mSegmentMeta.n);
  stripe->successBytes = 0;
  stripe->targetBytes = mSegmentMeta.n * Configuration::GetBlockSize() *
    mHeaderRegionSize; 
  stripe->totalTargetBytes = stripe->targetBytes;
  //mSegmentMeta.stripeSize;

  for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
    RequestContext *slot = mRequestContextPool->GetRequestContext(true);
    stripe->ioContext[i] = slot;
    slot->available = false;
  }

  if (!Configuration::GetEventFrameworkEnabled()) {
    thread_send_msg(mRaidController->GetEcThread(), finalizeSegmentHeader, this);
  } else {
    event_call(Configuration::GetEcThreadCoreId(), finalizeSegmentHeader2, this, nullptr);
  }
}

void Segment::FinalizeSegmentHeader()
{
  if (!Configuration::GetEnableHeaderFooter()) {
    mSegmentStatus = SEGMENT_NORMAL;
    return;
  }

  StripeWriteContext *stripe = mAdminStripe;
  uint32_t blockSize = Configuration::GetBlockSize();

  for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
    RequestContext *slot = stripe->ioContext[i];
    slot->associatedStripe = stripe;
    slot->targetBytes = blockSize * mHeaderRegionSize;
    slot->lba = ~0ull;
    slot->zoneId = i;
    slot->stripeId = 0;
    slot->segment = this;
    slot->ctrl = mRaidController;
    slot->status = WRITE_REAPING;
    slot->type = STRIPE_UNIT;
    slot->data = slot->dataBuffer;
    slot->meta = slot->metadataBuffer;
    slot->append = false;
    slot->offset = 0;
    slot->size = blockSize * mHeaderRegionSize;
    slot->zone = mZones[i];

    stripe->data[i] = slot->data;
    memcpy(stripe->data[i], &mSegmentMeta, sizeof(mSegmentMeta));

    debug_warn("Write header %p zone id %u offset %d zone %p\n", slot, slot->zoneId, 0, mZones[i]);
    StatsRecorder::getInstance()->IOBytesWrite(slot->size, i);
    mZones[i]->Write(0, slot->size, (void*)slot);
  }
  mPos += mHeaderRegionSize;
  mCurStripeId++;
}

void Segment::ProgressFooterWriter()
{
  // Currently it is the i-th stripe of the footer
  // 4096 / 20 = 204 LBAs
  // 8 bytes for LBA, 8 bytes write timestamp, and 4 bytes stripe ID
  // Each footer block contains blockSize / (8 + 8) entries
  // Thus the offset in the metadata array is "i * blockSize / 16"
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t unitSizeInBlks = mSegmentMeta.stripeUnitSize / blockSize;
  uint32_t begin = (mPos - mHeaderRegionSize - mDataRegionSize) * (blockSize / (8 + 8 + 4));
  uint32_t end = std::min(mDataRegionSize, begin + unitSizeInBlks * blockSize / (8 + 8 + 4));
  uint32_t footerNumBlks = unitSizeInBlks;
    // assume that the unit size is aligned
    //round_up(end - begin, blockSize / (8 + 8 + 4)) / (blockSize / (8 + 8 + 4));
  for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
    uint32_t base = i * mDataRegionSize;
    uint32_t pos = 0;
    for (uint32_t offset = begin; offset < end; ++offset) {
      uint8_t *footerForCurrentBlock = mCurStripe->ioContext[i]->dataBuffer + (offset - begin) * 20;
      *(uint64_t*)(footerForCurrentBlock + 0) = mCodedBlockMetadata[base + offset].lba;
      *(uint64_t*)(footerForCurrentBlock + 8) = mCodedBlockMetadata[base + offset].timestamp;

      uint32_t stripeId = (mSegmentMeta.useAppend) ? readCSTableOldIndex(base + offset) : 1;
      *(uint32_t*)(footerForCurrentBlock + 16) = stripeId;
    }
    for (uint32_t offset = begin; offset < end; ++offset) {
      uint8_t *footerForCurrentBlock = mCurStripe->ioContext[i]->dataBuffer + (offset - begin) * 20;
    }
  }
  mCurStripe->targetBytes = blockSize * footerNumBlks * mSegmentMeta.n;
  mCurStripe->successBytes = 0;
  for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
    RequestContext *slot = mCurStripe->ioContext[i];
    slot->status = WRITE_REAPING;
    slot->successBytes = 0;
    slot->offset = mPos;
    slot->zoneId = i;
    slot->data = slot->dataBuffer;
    slot->meta = slot->metadataBuffer;
    slot->zone = mZones[i];
    slot->size = blockSize * footerNumBlks;
    slot->targetBytes = blockSize * footerNumBlks;
    StatsRecorder::getInstance()->IOBytesWrite(slot->size, i);
    mZones[i]->Write(mPos, blockSize * footerNumBlks, (void*)slot);
  }
  mPos += footerNumBlks;
  mCurStripeId++;
}

bool Segment::findStripe()
{
  bool accept = false;
  if (mSegmentStatus == SEGMENT_NORMAL) {
    // Under normal appends, proceed

    // will recycle the request contexts associated with the stripe write
    // context
    StripeWriteContext *stripe = mStripeWriteContextPool->GetContext();
    if (stripe == nullptr) {
      if (Configuration::StuckDebugMode()) {
        debug_e("stuck: no stripe available");
      }
      accept = false;
    } else {
//      debug_w("found");
      mCurStripe = stripe;
      mCurStripe->successBytes = 0;

      // will be changed by Append() and PartialAppend() for raizn
      // Append(): change to mSegmentMeta.stripeSize
      // PartialAppend(): change to mPosInStripe
      mCurStripe->targetBytes = mSegmentMeta.stripeSize;
      mCurStripe->totalTargetBytes = mSegmentMeta.stripeSize;  // include meta zones

      // to be compatible with raizn
      mCurStripe->metaTargetBytes = 0;

      // old: prepare contexts for parity chunks 
      // new: preapre for all chunks. Prepare n+1 chunks because RAIZN needs parity updates
      mCurStripe->ioContext.resize(mSegmentMeta.n * 2 - mSegmentMeta.k);
      for (int j = 0; j < mSegmentMeta.n * 2 - mSegmentMeta.k; ++j) {
        RequestContext *context = mRequestContextPool->GetRequestContext(true);
        context->associatedStripe = mCurStripe;
        context->targetBytes = mSegmentMeta.stripeUnitSize;
        context->data = context->dataBuffer;
        context->meta = context->metadataBuffer;
        context->segment = this;
//        mCurStripe->ioContext[mSegmentMeta.k + j] = context;
        mCurStripe->ioContext[j] = context;
        if (j >= mSegmentMeta.n) {  // contexts for meta zones. Set to available first
          context->available = true;
        }
      }

      // calculate stripe write latencies
      {
        struct timeval tv;
        gettimeofday(&tv, 0);
        uint32_t latency = (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec -
          (uint64_t)mTimevalStripe.tv_sec * 1000000 - mTimevalStripe.tv_usec; 
        mStripeLatencies[latency]++;
        mTimevalStripe = tv;
      }

      accept = true;
    }
  }
  return accept;
}

struct GenerateParityBlockArgs {
  Segment *segment;
  StripeWriteContext *stripe;
  uint32_t zonePos;
  uint32_t stripeId;
  bool useMetaZone;
  // debug
  struct timeval stripeGenTv;
};

void generateParityBlock2(void *arg1, void *arg2)
{
  struct GenerateParityBlockArgs *gen_args = reinterpret_cast<struct GenerateParityBlockArgs*>(arg1);
  Segment *segment = reinterpret_cast<Segment*>(gen_args->segment);
  StripeWriteContext *stripe = reinterpret_cast<StripeWriteContext*>(gen_args->stripe);
  uint32_t zonePos = gen_args->zonePos;
  uint32_t stripeId = gen_args->stripeId;
  bool useMetaZone = gen_args->useMetaZone;
  struct timeval stripeGenTv = gen_args->stripeGenTv;
  free(gen_args);
  segment->GenerateParityBlock(stripe, zonePos, stripeId, stripeGenTv, useMetaZone);
}

void generateParityBlock(void *args)
{
  generateParityBlock2(args, nullptr);
}

void Segment::FlushCurrentStripe()
{
  static uint64_t flushTimes = 0;
  uint32_t lastPos = mPos;
  uint32_t lastPosInStripe = mPosInStripe;
  while (HasInFlightStripe()) {
    if (flushTimes % 1000000 == 0) {
      debug_error("flushing %lu\n", flushTimes);
    }
    flushTimes++;
    STAT_TIME_PROCESS(BufferedAppend(nullptr, 0, 0), StatsType::FLUSH_CURRENT_STRIPE);
  }
}

inline uint32_t Segment::readCSTableOldIndex(uint32_t offset) {
  return readCSTable(offset / (mSegmentMeta.stripeUnitSize / Configuration::GetBlockSize()));
}

inline uint32_t Segment::readCSTable(uint32_t index) {
  uint32_t stripeId;
  if (mSegmentMeta.stripeGroupSize <= 256) {
    stripeId = mCompactStripeTable[index];
  } else if (mSegmentMeta.stripeGroupSize <= 65536) {
    stripeId = ((uint16_t*)mCompactStripeTable)[index];
  } else {
    uint8_t *tmp = mCompactStripeTable + index * 3;
    stripeId = (*(tmp + 2) << 16) | *(uint16_t*)tmp;
  }
  return stripeId;
}

inline void Segment::writeCSTableOldIndex(uint32_t index, uint32_t stripeId) {
  writeCSTable(index / (mSegmentMeta.stripeUnitSize / Configuration::GetBlockSize()), stripeId);
}

inline void Segment::writeCSTable(uint32_t index, uint32_t stripeId) {
  if (mSegmentMeta.stripeGroupSize <= 256) {
    mCompactStripeTable[index] = stripeId;
  } else if (mSegmentMeta.stripeGroupSize <= 65536) {
    ((uint16_t*)mCompactStripeTable)[index] = stripeId;
  } else {
    uint8_t *tmp = mCompactStripeTable + index * 3;
    *(tmp + 2) = stripeId >> 16;
    *(uint16_t*)tmp = stripeId & 0xffff;
  }
}

// RAID controller Call this function. If only half is written, it only returns
// the successfully written size. Then RAID controller will call this function
// and continue to write.
uint32_t Segment::BufferedAppend(RequestContext *ctx, uint32_t offset, uint32_t size)
{
  static int first = 0, second = 0;
//  debug_w("start");
  mTimevalBetweenAppend = mTimeval;
  gettimeofday(&mTimeval, 0);
  // can be the same as append

  // may be resetting, wait for the reset to complete
  if (Configuration::GetSystemMode() == RAIZN_SIMPLE && 
      mSegmentStatus != SEGMENT_NORMAL) {
    return 0;
  }

  if (Configuration::StuckDebugMode()) {
    debug_error("mPosInStripe %u mPos %u slot %p\n",
        mPosInStripe, mPos, mCurSlot);
  }

  if (!first) {
    debug_warn("here %d\n", mOngoingRaiznWrites);
    first = 1;
  }
  if (Configuration::GetSystemMode() == RAIZN_SIMPLE &&
      mOngoingRaiznWrites > 0) {
    return 0;
  }
//  debug_w("here");

  if (mPosInStripe == 0 && mCurSlot == nullptr) {
    if (!findStripe()) {
      return 0;
    }
    mLastStripeCreationTimestamp = GetTimestampInUs();
  }

  struct timeval s, e;
  gettimeofday(&s, 0);
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t unitSize = mSegmentMeta.stripeUnitSize;
  assert(size % blockSize == 0);

  uint64_t lba = ~0ull;
  uint8_t *blkdata = nullptr;
  uint32_t whichStripe = mPosInStripe / unitSize;

  if (ctx) {
    lba = ctx->lba + offset * blockSize;
    blkdata = (uint8_t*)(ctx->data) + offset * blockSize;
  }

  uint32_t zoneId = Configuration::CalculateDiskId(mCurStripeId, whichStripe, 
    mSegmentMeta.raidScheme, mSegmentMeta.numZones);

  RequestContext* slot = mCurSlot;
  if (mCurSlot == nullptr) {
    // changed for RAIZN; prepare all slots first
//    mCurSlot = mRequestContextPool->GetRequestContext(true);
//    slot = mCurSlot;
//    slot->size = 0;
//    slot->curOffset = 0;
//    mCurStripe->ioContext[whichStripe] = slot;

    mCurSlot = mCurStripe->ioContext[whichStripe];
    slot = mCurSlot;
    slot->type = STRIPE_UNIT;
    slot->status = WRITE_REAPING;
    slot->size = 0;       // unnecessary
    slot->curOffset = 0;  // unnecessary

    // this lba is replaced with lbaArray
    // slot->lba = lba;

    slot->timestamp = ctx ? ctx->timestamp : ~0ull;
    slot->data = slot->dataBuffer;
    slot->meta = slot->metadataBuffer;
    slot->associatedStripe = mCurStripe;

    slot->zoneId = zoneId;
    // the stripe Id within the group
    slot->stripeId = mSegmentMeta.useAppend ? ((mPos - mHeaderRegionSize) / (unitSize / blockSize) % mSegmentMeta.stripeGroupSize) : 0;
    slot->segment = this;
    slot->ctrl = mRaidController;
  }

  uint32_t uCtxSize = 0;
  uint32_t originalSize = slot->size;

  debug_warn("slot size %u lba %lu unit size %u\n", slot->size,
      slot->lba,
      mSegmentMeta.stripeUnitSize);

  struct timeval tvHalf;
  gettimeofday(&tvHalf, 0);

  if (Configuration::StuckDebugMode()) {
    debug_error("slot size %u lba %lu unit size %u size %u\n", slot->size,
        slot->lba,
        mSegmentMeta.stripeUnitSize, size);
  }

  // append to slot buffer
  if (slot->size + size <= mSegmentMeta.stripeUnitSize) {
    // why data and dataBuffer? TODO
    // Append to the buffer
    if (blkdata) {
      uCtxSize = size;
//      memcpy(slot->data + slot->size, blkdata, uCtxSize);
      if (ctx->type == USER) {
        for (int nBytes = 0; nBytes < size; nBytes += blockSize) {
          uint64_t blockLba = ctx->lba + offset * blockSize + nBytes;
          slot->lbaArray[(slot->size + nBytes) / blockSize] = blockLba;
        }
      } else if (ctx->type == GC) {
        for (int nBytes = 0; nBytes < size; nBytes += blockSize) {
          uint64_t blockLba = ctx->lbaArray[offset + nBytes / blockSize];
          if (blockLba == ~0ull) {
            debug_error("Gc write lba cannot be 0! nBytes %u ctx %p",
              nBytes, ctx);
            assert(0);
          }
          slot->lbaArray[(slot->size + nBytes) / blockSize] = blockLba;
        }
      } else if (ctx->type == INDEX) {
        // assume that it only writes one index block
        assert(offset == 0 && size == blockSize);
        uint64_t lba = round_down(ctx->lba, blockSize * blockSize / 4);
        // set all 22 bits to 1
        lba = lba + (blockSize * blockSize / 4) - 1;
        slot->lbaArray[slot->size / blockSize] = lba;
      } else {
        assert(false);
      }
    } else {
      // block size is empty, flush the block
      uCtxSize = mSegmentMeta.stripeUnitSize - slot->size;
//      memset(slot->data + slot->size, 0, uCtxSize);
      for (int nBytes = 0; nBytes < uCtxSize; nBytes += blockSize) {
        slot->lbaArray[(slot->size + nBytes) / blockSize] = ~0ull;
      }
    }
    slot->size += uCtxSize;
  } else {
    uCtxSize = mSegmentMeta.stripeUnitSize - slot->size;
    if (blkdata) {
      if (ctx->type == USER) {
        for (int nBytes = 0; nBytes < uCtxSize; nBytes += blockSize) {
          uint64_t lba = ctx->lba + offset * blockSize + nBytes;
          slot->lbaArray[(slot->size + nBytes) / blockSize] = lba;
        }
      } else if (ctx->type == GC) {
        for (int nBytes = 0; nBytes < uCtxSize; nBytes += blockSize) {
          uint64_t lba = ctx->lbaArray[offset + nBytes / blockSize];
          assert(lba != ~0ull);
          slot->lbaArray[(slot->size + nBytes) / blockSize] = lba;
        }
      } else if (ctx->type == INDEX) {
        assert(offset == 0 && size == blockSize);
        uint64_t lba = round_down(ctx->lba, blockSize * blockSize / 4);
        // set all 22 bits to 1
        lba = lba + (blockSize * blockSize / 4) - 1;
        slot->lbaArray[slot->size / blockSize] = lba;
      } else {
        assert(false);
      }
    } else {
      for (int nBytes = 0; nBytes < uCtxSize; nBytes += blockSize) {
        slot->lbaArray[(slot->size + nBytes) / blockSize] = ~0ull;
      }
    }
    slot->size = mSegmentMeta.stripeUnitSize;
  }

  slot->associatedRequests.push_back({ctx, {offset, uCtxSize}});
  mLastAppendTimestamp = GetTimestampInUs();
  uint32_t ret = 0;

  if (slot->size == mSegmentMeta.stripeUnitSize) {
    debug_warn("slot full size %u\n", slot->size);
    // prepare slot; most of them has been ready before

    ret = 0;

    if (Configuration::GetSystemMode() == RAIZN_SIMPLE) {
      // the last part for the write, write a PPU
      bool mayHavePpu = (offset * blockSize + uCtxSize == ctx->size); 
      ret = PartialAppend(mayHavePpu);
    } else {
      ret = Append();
    }

    if (ret == mSegmentMeta.stripeUnitSize) {
      debug_warn("slot full return %u\n", ret - originalSize);
      ret = ret - originalSize;
    } else {
      // write failed
      ret = 0;
    }
  } else {
    debug_warn("slot not full size %u curOffset %u\n", slot->size, slot->curOffset);
    ret = uCtxSize;
    if (Configuration::GetSystemMode() == RAIZN_SIMPLE) {
      // the last part for the write, write a PPU
      bool mayHavePpu = (offset * blockSize + uCtxSize == ctx->size);
      ret = PartialAppend(mayHavePpu);
      if (ret == slot->size) {
        ret -= originalSize;
      } else {
        ret = 0;
      }
    }
    debug_warn("slot not full size %u return %u\n", slot->size, uCtxSize);
    // the buffer is not full. Only return the buffered size
  }

  return ret;
}

// The append size must be the same as the unit size
uint32_t Segment::Append()
{
  SystemMode mode = Configuration::GetSystemMode();
  uint32_t blockSize = Configuration::GetBlockSize();

  struct timeval s;
  gettimeofday(&s, 0);
  {
    assert(mCurSlot != nullptr);
    RequestContext *slot = mCurSlot;

    // Initialize block (flash page) metadata
    for (int lbaCnt = 0; lbaCnt < mSegmentMeta.stripeUnitSize / blockSize; lbaCnt++) {
      // 64-byte offset for each LBA count
      BlockMetadata *blkMeta = (BlockMetadata *)slot->meta + lbaCnt;
      blkMeta->fields.coded.lba = slot->lbaArray[lbaCnt];
      blkMeta->fields.coded.timestamp = slot->timestamp;
      blkMeta->fields.replicated.stripeId = slot->stripeId;
    }

    slot->append = mSegmentMeta.useAppend; 

    // breakdown
    {
      struct timeval tv; 
      gettimeofday(&tv, 0);
      uint64_t time_in_us = tv.tv_sec * 1000000 + tv.tv_usec - mTimeval.tv_sec *
        1000000 - mTimeval.tv_usec;
      mAppendLatencies[time_in_us]++;
    }

    // add these lines to be compatible with raizn 
    // start writing from curOffset 
    slot->targetBytes = mSegmentMeta.stripeUnitSize;
    uint32_t bytes2append = slot->targetBytes - slot->curOffset * blockSize; 
    mCurStripe->targetBytes = mSegmentMeta.stripeSize;

    // asynchronous write
    // change stripeUnitSize to bytes2append to be compatible with raizn
    slot->zone = mZones[slot->zoneId];
    StatsRecorder::getInstance()->IOBytesWrite(bytes2append, slot->zoneId);
//    struct timeval s;
//    gettimeofday(&s, 0);
    slot->zone->Write(mPos + slot->curOffset, bytes2append, (void*)slot);
//    struct timeval e;
//    gettimeofday(&e, 0);
//    mRaidController->MarkDispatchThreadTestTime(s, e);
//    slot->curOffset = slot->size / blockSize; 

    // after write, clean the current slot
    mCurSlot = nullptr;

    // debug
    if (Configuration::GetEnableIOLatencyTest()) {
      int idx = mPosInStripe / mSegmentMeta.stripeUnitSize;
      if (idx < 4) {
        struct timeval tv;
        gettimeofday(&tv, 0);
        // time between mTimevalStripe and tv
        uint64_t time_in_us = tv.tv_sec * 1000000 + tv.tv_usec - mTimevalStripe.tv_sec *
          1000000 - mTimevalStripe.tv_usec;
        mStripeStartWriteLatencies[idx][time_in_us]++;
      }
    }

    mPosInStripe += bytes2append;
  }

  if (mPosInStripe == mSegmentMeta.stripeDataSize) {
    // issue parity block
    GenerateParityBlockArgs *args = (GenerateParityBlockArgs*)calloc(1,
        sizeof(GenerateParityBlockArgs));
    args->segment = this;
    args->stripe = mCurStripe;
    args->zonePos = mPos;
    args->stripeId = mCurStripeId;
    args->stripeGenTv = mTimevalStripe; 
    args->useMetaZone = false;

    if (!Configuration::GetEventFrameworkEnabled()) {
      thread_send_msg(mRaidController->GetEcThread(), generateParityBlock, args);
    } else {
      event_call(Configuration::GetEcThreadCoreId(), generateParityBlock2, args, nullptr);
    }

    mPosInStripe = 0;
    mPos += mSegmentMeta.stripeUnitSize / blockSize;
    mCurStripeId++;

    if (mode == ZAPRAID) {
      // need to check append; otherwise the zone write will be slow
      if (mSegmentMeta.useAppend && 
          (mPos - mHeaderRegionSize) % (mSegmentMeta.stripeGroupSize *
          mSegmentMeta.stripeUnitSize / blockSize) == 0) {
        mSegmentStatus = SEGMENT_CONCLUDING_APPENDS_IN_GROUP;
      }
    }

    if (mPos == mHeaderRegionSize + mDataRegionSize) {
      // writing the P2L table at the end of the segment
      mSegmentStatus = SEGMENT_PREPARE_FOOTER;
    }
  }

  return mSegmentMeta.stripeUnitSize;
}

uint32_t Segment::PartialAppend(bool mayHavePpu)
{
  static struct timeval tv1, tv2;
  static int cnt = 0;
  if (cnt == 0) tv1.tv_sec = 0, cnt = 1;

  debug_w("start");
  if (Configuration::GetSystemMode() != RAIZN_SIMPLE) {
    debug_e("only supported in RAIZN_SIMPLE mode");
    assert(0);
  }
  uint32_t blockSize = Configuration::GetBlockSize();

  assert(mCurSlot != nullptr);
  RequestContext *slot = mCurSlot;

  {
    // Initialize block (flash page) metadata
    for (int lbaCnt = slot->curOffset; lbaCnt < slot->size / blockSize; lbaCnt++) {
      BlockMetadata *blkMeta = (BlockMetadata *)slot->meta + lbaCnt;
      blkMeta->fields.coded.lba = slot->lbaArray[lbaCnt];
      blkMeta->fields.coded.timestamp = slot->timestamp;
      blkMeta->fields.replicated.stripeId = slot->stripeId;
    }

    slot->append = mSegmentMeta.useAppend; 

    // breakdown
    {
      struct timeval tv; 
      gettimeofday(&tv, 0);
      uint64_t time_in_us = tv.tv_sec * 1000000 + tv.tv_usec - mTimeval.tv_sec *
        1000000 - mTimeval.tv_usec;
      mAppendLatencies[time_in_us]++;
    }

    // add these lines to be compatible with raizn 
    // start writing from curOffset
    // note that Append() and PartialAppend() do not touch successBytes
    assert(slot->size <= mSegmentMeta.stripeUnitSize);
//    debug_error("used before? %p %d curOffset %u\n", slot, slot->available, slot->curOffset);

    // reset the variables so that it can be reused
    slot->available = false; // may be used before, need to set to false again
    slot->status = WRITE_REAPING;

    slot->targetBytes = slot->size;
    uint32_t bytes2append = slot->size - slot->curOffset * blockSize;
    mCurStripe->targetBytes = mPosInStripe + bytes2append;

    totalStripeWrites += bytes2append;
    mPosInStripe += bytes2append;

    // a full stripe. Need to change the target bytes to a whole stripe
    if (mCurStripe->targetBytes == mSegmentMeta.stripeDataSize) {
      totalStripeWrites += mSegmentMeta.stripeSize - mCurStripe->targetBytes;
      mCurStripe->targetBytes = mSegmentMeta.stripeSize;
    } else {
      // ... or write a ppu in metadata zone.
      // We assume that RAIDController will fill the in-flight stripes to
      // a segment first until creating other in-flight stripes. So it is
      // impossible that some slots are written to a non-aligned stripe without
      // a PPU while other slots are written to another non-aligned stripe with
      // a PPU
      if (mayHavePpu && mPosInStripe != mSegmentMeta.stripeDataSize) {
        mCurStripe->metaTargetBytes += mSegmentMeta.stripeUnitSize;
        totalMetaWrites += mSegmentMeta.stripeUnitSize;
        // force each user write to finish after a PPU or a parity is written
      }

      if (mayHavePpu) {
        // if the slot is the last slot of the user write, need to stop all
        // following writes to the same segment.
        // Note that it is OK that a large write whose PPU is written to a
        // segment while other data is written to another segment, and the
        // segment with the PPU finish first. In this case, the user write
        // still needs to wait for another segment to finish the write.
        mOngoingRaiznWrites++;
      }
    }

    debug_warn("bytes2append %u slot %p zone id %u stripe.target %u metatarget %u\n",
        bytes2append, slot, slot->zoneId, mCurStripe->targetBytes,
        mCurStripe->metaTargetBytes);

    debug_warn("total writes: %lu total meta writes %lu\n",
        totalStripeWrites, totalMetaWrites);

//    gettimeofday(&tv2, 0);
//    if (tv2.tv_sec > tv1.tv_sec) {
//      tv1 = tv2;
//      debug_error("total writes: %lu total meta writes %lu\n",
//          totalStripeWrites, totalMetaWrites);
//    }

    debug_warn("mayHavePpu: %d\n", (int)mayHavePpu);

    // asynchronous write
    slot->zone = mZones[slot->zoneId];
    StatsRecorder::getInstance()->IOBytesWrite(bytes2append, slot->zoneId);
    slot->zone->Write(mPos + slot->curOffset, bytes2append, (void*)slot);

    // after write, clean the current slot if it is full
    if (slot->size == mSegmentMeta.stripeUnitSize) {
      mCurSlot = nullptr;
    } 
  }

  do {
    // Check whether to write PPU or a complete parity.
    if (!mayHavePpu && mPosInStripe != mSegmentMeta.stripeDataSize) {
      // !mayHavePpu: Do not skip PPU if it is not the final slot of the write
      // (mayHavePpu: is the final slot of the write)
      // !=: Do not skip a complete parity if the stripe is not full 
      // (==: The stripe is full)
      break;
    }
    // issue PPUs to metadata zone, or full parity writes to regular zones 
    GenerateParityBlockArgs *args = (GenerateParityBlockArgs*)calloc(1,
        sizeof(GenerateParityBlockArgs));
    args->segment = this;
    args->stripe = mCurStripe;
    args->zonePos = mPos;
    args->stripeId = mCurStripeId;
    args->stripeGenTv = mTimevalStripe; 
    args->useMetaZone = (mPosInStripe != mSegmentMeta.stripeDataSize);

    debug_warn("prepare parity block %p zonePos %u stripeId %u\n", mCurStripe, mPos, mCurStripeId);

    bool useMetaZone = args->useMetaZone;

    if (!Configuration::GetEventFrameworkEnabled()) {
      thread_send_msg(mRaidController->GetEcThread(), generateParityBlock, args);
    } else {
      event_call(Configuration::GetEcThreadCoreId(), generateParityBlock2, args, nullptr);
    }

    // whole stripe written
    if (!useMetaZone) {
      mPosInStripe = 0;
      mPos += mSegmentMeta.stripeUnitSize / blockSize;
      mCurStripeId++;

      if (mPos == mHeaderRegionSize + mDataRegionSize) {
        // writing the P2L table at the end of the segment
        mSegmentStatus = SEGMENT_PREPARE_FOOTER;
      }
    }
  } while (0);

  return slot->size;
}

void Segment::GenerateParityBlock(StripeWriteContext *stripe, uint32_t zonePos, uint32_t stripeId,
    struct timeval stripeGenTv, bool useMetaZone)
{
  debug_warn("GenerateParityBlock %p zonePos %u\n", stripe, zonePos);
  static double accummulated = 0;
  static int count = 0;
  struct timeval s, e;
  // gettimeofday(&s, NULL);
  uint32_t n = mSegmentMeta.n;
  uint32_t k = mSegmentMeta.k;

  uint8_t* stripeData[n];
  uint8_t* stripeProtectedMetadata[n];
  uint32_t unitSize = mSegmentMeta.stripeUnitSize;
  uint32_t blockSize = Configuration::GetBlockSize();

  if (mSegmentMeta.raidScheme == RAID0) {
    // NO PARITY
  } else if (mSegmentMeta.raidScheme == RAID1) {
    for (uint32_t i = k; i < n; ++i) {
      memcpy(stripe->ioContext[i]->data, stripe->ioContext[0]->data, unitSize);
    }
  } else if (mSegmentMeta.raidScheme == RAID01) {
    for (uint32_t i = k; i < n; ++i) {
      memcpy(stripe->ioContext[i]->data, stripe->ioContext[i - k]->data, unitSize);
    }
  } else if (mSegmentMeta.raidScheme == RAID4
      || mSegmentMeta.raidScheme == RAID5
      || mSegmentMeta.raidScheme == RAID6) {
    for (uint32_t i = 0; i < n; ++i) {
      stripeData[i] = stripe->ioContext[i]->data;
      stripeProtectedMetadata[i] = reinterpret_cast<uint8_t*>(
          &(((BlockMetadata*)stripe->ioContext[i]->meta)->fields.coded));
    }
    EncodeStripe(stripeData, n, k, unitSize);
    EncodeStripe(stripeProtectedMetadata, n, k, Configuration::GetMetadataSize() * 
      unitSize / blockSize); // actual 16 bytes for the protected fields, but encode all
  }

  for (uint32_t i = k; i < n; ++i) {
    uint32_t zoneId = Configuration::CalculateDiskId(
        stripeId, i, mSegmentMeta.raidScheme, mSegmentMeta.numZones  
        /*zonePos, i, (RAIDLevel)mSegmentMeta.raidScheme, mSegmentMeta.numZones*/);
    RequestContext *slot = stripe->ioContext[i];
    if (useMetaZone) {
      slot = stripe->ioContext[i - k + n];
      // TODO copy the parity data from the ioContext[i] to ioContext[i - k + n]
    }
    slot->lba = ~0ull;
    slot->ctrl = mRaidController;
    slot->segment = this;
    slot->zoneId = zoneId;
    slot->stripeId = (stripeId - 1) % mSegmentMeta.stripeGroupSize;
    slot->status = WRITE_REAPING;
    slot->type = STRIPE_UNIT;
    slot->append = mSegmentMeta.useAppend; 
    slot->successBytes = 0;

    for (int lbaCnt = 0; lbaCnt < unitSize / blockSize; lbaCnt++) {
      BlockMetadata *blkMeta = (BlockMetadata *)(slot->meta) + lbaCnt;
      blkMeta->fields.replicated.stripeId = slot->stripeId;
    }

    slot->size = unitSize;
    if (useMetaZone) {
      debug_warn("meta zone: slot %p i %d zoneId %d\n", slot, i, zoneId);
      assert(slot->available);
      slot->available = false;
      // both the dispatch thread and encoding thread use the meta zone list
      // If the metadata zone is full, wait until RAIDController removes the
      // zone from the list
      Zone *metaZone = nullptr;
      while (true) {
        metaZone = mMetaZoneList[zoneId].front();
        if (!metaZone->WillBeFull()) break; 
      }
      assert(metaZone != nullptr);
      auto metaZonePos = metaZone->GetIssuedPos();  
//      if (metaZonePos % 32768 == 0) {
//        debug_error("segment %lu slot %p i %d devId %d pos %u\n", 
//          mSegmentMeta.segmentId, slot, i, zoneId, metaZonePos);
//      }
      slot->append = true;
      slot->zone = metaZone;
      slot->zone->Write(metaZonePos, unitSize, (void*)slot);
    } else {
      slot->zone = mZones[zoneId];
      mZones[zoneId]->Write(zonePos, unitSize, slot);
    }

    // debug
    if (Configuration::GetEnableIOLatencyTest()) {
      int idx = mSegmentMeta.k;
      if (idx < 4) {
        struct timeval tv;
        gettimeofday(&tv, 0);
        // time between mTimevalStripe and tv
        uint64_t time_in_us = tv.tv_sec * 1000000 + tv.tv_usec - stripeGenTv.tv_sec *
          1000000 - stripeGenTv.tv_usec;
        mStripeStartWriteLatencies[idx][time_in_us]++;
      }
    }
  }
}

// do not want to change this function
bool Segment::Read(RequestContext *ctx, uint32_t pos, PhysicalAddr phyAddr, uint32_t size)
{
  ReadContext *readContext = mReadContextPool->GetContext();
  if (readContext == nullptr) {
    return false;
  }

  uint32_t blockSize = Configuration::GetBlockSize();
  RequestContext *slot = mRequestContextPool->GetRequestContext(true);
  slot->associatedRead = readContext;
  slot->available = false;
  slot->associatedRequests.clear();
  slot->associatedRequests.push_back({ctx, {pos, size}});
  slot->lba = ctx->lba + pos * blockSize;
  slot->targetBytes = size;
  slot->zoneId = phyAddr.zoneId;
  slot->offset = phyAddr.offset;
  slot->ctrl = mRaidController;
  slot->segment = this;
  slot->status = READ_REAPING;
  slot->type = STRIPE_UNIT;
  // to make sure that the buffer is aligned in memory
  if (size > slot->bufferSize) {
    slot->bufferSize = round_up(size, blockSize);
    spdk_free(slot->dataBuffer);
    spdk_free(slot->metadataBuffer);
    slot->dataBuffer = (uint8_t*)spdk_zmalloc(
        slot->bufferSize, 
        4096, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    slot->metadataBuffer = (uint8_t*)spdk_zmalloc(
        slot->bufferSize / blockSize * Configuration::GetMetadataSize(),
        4096, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    if (slot->dataBuffer == nullptr || slot->metadataBuffer == nullptr) {
      debug_error("Cannot allocate memory: size %u new bufferSize %u\n",
          size, slot->bufferSize);
      assert(0);
    }
  }
  slot->data = (uint8_t*)(slot->dataBuffer); 
  slot->meta = (uint8_t*)(slot->metadataBuffer);

  slot->successBytes = 0;
  slot->size = size;

  readContext->data[readContext->ioContext.size()] = (uint8_t*)slot->data;
  readContext->ioContext.emplace_back(slot);

  uint32_t unitSizeInBlks = mSegmentMeta.stripeUnitSize / blockSize;

  slot->needDegradedRead = Configuration::GetEnableDegradedRead();
  if (slot->needDegradedRead) {
    slot->Queue();
  } else {
    uint32_t zoneId = slot->zoneId;
    uint32_t offset = slot->offset;

//    if (offset / unitSizeInBlks != (offset + size / blockSize - 1) / unitSizeInBlks) {
//      debug_error("Read size %u %u crosses stripe boundary\n", offset, size);
//      printf("Read size %u %u crosses stripe boundary\n", offset, size);
//    }

    slot->zone = mZones[zoneId];
    StatsRecorder::getInstance()->IOBytesRead(size, zoneId);
    mZones[zoneId]->Read(offset, size, slot);
  }

  return true;
}

bool Segment::ReadValid(RequestContext *ctx, uint32_t pos, PhysicalAddr phyAddr, uint32_t size)
{
  bool success = false;
  uint32_t zoneId = phyAddr.zoneId;
  uint32_t offset = phyAddr.offset;

  assert(size < 64 * Configuration::GetBlockSize()); // validBits is only for 64 blocks

  ctx->nValid = 0;
  ctx->validBits = 0;

  for (int i = 0; i < size / Configuration::GetBlockSize(); ++i) {
    if (offset + i < mHeaderRegionSize) continue;
    if (offset + i >= mHeaderRegionSize + mDataRegionSize) break; 
    uint64_t base = zoneId * mDataRegionSize + offset - mHeaderRegionSize + i;
    if ((mValidBits[base / 8] & (1 << (base % 8))) != 0) {
      ctx->nValid++;
      ctx->validBits |= (1 << i);
    }
  }
  if (ctx->nValid == 0) {
    success = true;
  } else {
    success = Read(ctx, pos, phyAddr, size);
  }
  return success;
}

void Segment::Reset(RequestContext *ctx)
{
  mResetContext.resize(mSegmentMeta.n);
  for (int i = 0; i < mSegmentMeta.n; ++i) {
    RequestContext *context = &mResetContext[i];
    context->Clear();
    context->associatedRequests.push_back({ctx, {0, mSegmentMeta.stripeUnitSize}});
    context->ctrl = mRaidController;
    context->segment = this;
    context->type = STRIPE_UNIT;
    context->status = RESET_REAPING;
    context->available = false;
    mZones[i]->Reset(context);
  }
}

bool Segment::IsResetDone()
{
  bool done = true;
  for (auto context : mResetContext) {
    if (!context.available) {
      done = false;
      break;
    }
  }
  return done;
}

void Segment::Seal()
{
  if (mCurStripe->ioContext.empty()) {
    mCurStripe->ioContext.resize(mSegmentMeta.n);
    for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
      mCurStripe->ioContext[i] = mRequestContextPool->GetRequestContext(true);
    }
  }
  for (uint32_t i = 0; i < mSegmentMeta.n; ++i) {
    RequestContext *context = mCurStripe->ioContext[i];
    context->Clear();
    context->available = false;
    context->ctrl = mRaidController;
    context->segment = this;
    context->type = STRIPE_UNIT;
    context->status = FINISH_REAPING;
    mZones[i]->Seal(context);
  }
  printf("Seal segment ID %u, usage %u/%u = %.2lf, index blocks %u\n", 
      GetSegmentId(), mNumBlocks, mDataRegionSize * 3, 
      (double)mNumBlocks / (mDataRegionSize * 3), mNumIndexBlocks);
}

void Segment::ReadStripe(RequestContext *ctx)
{
  SystemMode mode = Configuration::GetSystemMode();
  struct timeval tv0;
  gettimeofday(&tv0, 0);

  // ctx is a STRIPE_UNIT context generated in Read(), it specifies the
  // physical address of the data to be degraded-read 
  uint32_t zoneId = ctx->zoneId;
  uint32_t offset = ctx->offset;
  uint32_t n = mSegmentMeta.n;
  uint32_t k = mSegmentMeta.k;
  ReadContext *readContext = ctx->associatedRead;
  uint32_t realOffsets[n];
  uint32_t unitSize = mSegmentMeta.stripeUnitSize; 
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t unitSizeInBlks = unitSize / blockSize;
  uint32_t inStripeSize = ctx->size; 

  uint64_t inChunkOffset = (offset - mHeaderRegionSize) % (unitSize / blockSize);
  uint32_t dataRegionStripes = mDataRegionSize / unitSizeInBlks;

  if (mode == ZAPRAID) {
    uint32_t groupSizeInBlocks = mSegmentMeta.stripeGroupSize * unitSizeInBlks;
    uint32_t groupId = (offset - mHeaderRegionSize) / groupSizeInBlocks;
    uint32_t searchBegin = groupId * groupSizeInBlocks;
    uint32_t searchEnd = std::min(searchBegin + groupSizeInBlocks, mPos - mHeaderRegionSize);

    // Find out the stripeId of the requested block
    uint32_t index = zoneId * mDataRegionSize + offset - mHeaderRegionSize;

    uint32_t stripeId = readCSTableOldIndex(index);

    ctx->stripeId = stripeId;

    // Search the stripe ID table
    for (uint32_t i = 0; i < n; ++i) {
      realOffsets[i] = ~0u;
      if (i == zoneId) {
        // data to be read
        realOffsets[i] = offset;
        continue;
      }
//      debug_error("searchBegin %u searchEnd %u\n", searchBegin, searchEnd);
      for (uint32_t j = searchBegin + inChunkOffset; j < searchEnd; j += unitSizeInBlks) {
        // index of CS table, not the offset
        uint32_t index = i * dataRegionStripes + j / unitSizeInBlks;
        uint32_t stripeIdOfCurBlock = ~0u; 
        if (mSegmentMeta.stripeGroupSize <= 256) {
          stripeIdOfCurBlock = mCompactStripeTable[index];
        } else if (mSegmentMeta.stripeGroupSize <= 65536) {
          stripeIdOfCurBlock = ((uint16_t*)mCompactStripeTable)[index];
        } else {
          uint8_t *tmp = mCompactStripeTable + index * 3;
          stripeIdOfCurBlock = (*(tmp + 2) << 16) | *(uint16_t*)tmp;
        }

//        debug_error("index %u stripeIdOfCurBlock %u stripeId %u j %u\n", index,
//            stripeIdOfCurBlock, stripeId, j);

        if (stripeIdOfCurBlock == stripeId) {
          if (realOffsets[i] != ~0u) {
            printf("Duplicate stripe ID %p %u %u %u offset %u!\n", this, index,
                stripeIdOfCurBlock, stripeId, realOffsets[i]);
            assert(0);
//            uint8_t *tmp = mCompactStripeTable + index * 3;
//            printf("%p %p %p %lu %lu\n", this, tmp, mCompactStripeTable, index, *(tmp + 2), *(uint16_t*)tmp);
          }
          realOffsets[i] = mHeaderRegionSize + j;
          break;
        }
      }
      if (realOffsets[i] == ~0u) {
        printf("Not find the stripe ID!\n");
        assert(0);
      }
    }
  } else if (mode == ZONEWRITE_ONLY) {
    for (uint32_t i = 0; i < n; ++i) {
      realOffsets[i] = offset;
    }
  }

  {
    struct timeval tv;
    gettimeofday(&tv, 0);
    uint32_t latency = (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec -
      (uint64_t)tv0.tv_sec * 1000000 - tv0.tv_usec;
    mAppendHalfLatencies[latency]++;
  }

  ctx->successBytes = 0;
  ctx->targetBytes = k * inStripeSize;
  uint32_t cnt = 0;
  for (uint32_t i = 0; i < n && cnt < k; ++i) {
    // skip the object zone (the degraded-read or lost zone)
    if (i == zoneId) continue;

    RequestContext *reqCtx = nullptr;
    reqCtx = mRequestContextPool->GetRequestContext(true);
    readContext->ioContext.emplace_back(reqCtx);

    reqCtx->Clear();
    // TODO check the offset
    reqCtx->associatedRequests.push_back({ctx, {0, inStripeSize}});
    reqCtx->status = DEGRADED_READ_SUB;
    reqCtx->type = STRIPE_UNIT;
    reqCtx->targetBytes = inStripeSize;
    reqCtx->ctrl = mRaidController;
    reqCtx->segment = this;
    reqCtx->zoneId = i;
    reqCtx->offset = realOffsets[i];
    reqCtx->data = reqCtx->dataBuffer;
    reqCtx->meta = reqCtx->metadataBuffer;

    readContext->data[i] = reqCtx->data;

    reqCtx->size = inStripeSize;
    reqCtx->successBytes = 0;

    reqCtx->zone = mZones[i];
    StatsRecorder::getInstance()->IOBytesRead(inStripeSize, reqCtx->zoneId);
    mZones[i]->Read(realOffsets[i], inStripeSize, reqCtx);

    ++cnt;
  }
  StatsRecorder::getInstance()->timeProcess(StatsType::DEGRADED_READ_CSTABLE, tv0);
}

void Segment::WriteComplete(RequestContext *slot)
{
  debug_warn("slot->curOffset %u slot->size %u\n", slot->curOffset, slot->size);
  if (slot->offset < mHeaderRegionSize ||
      slot->offset >= mHeaderRegionSize + mDataRegionSize) {
    return;
  }

  // modified because raizn needs partial stripe writes
  // ioOffset can also be curOffset
  uint32_t index = slot->zoneId * mDataRegionSize + slot->offset + slot->ioOffset - mHeaderRegionSize;

  uint32_t blockSize = Configuration::GetBlockSize();
  // Note that here the (lba, timestamp, stripeId) is not the one written to flash page
  // Thus the lba and timestamp is "INVALID" for parity block, and the footer
  // (which uses this field to fill) is valid
  
  // update the block metadata for all blocks
  uint32_t base = index;
  uint32_t pos = 0;
  bool isParity = false;

  // LBA and timestamp
  for (auto& it : slot->associatedRequests) {
    RequestContext *parent = it.first;
    uint64_t uCtxOffset = it.second.first;
    uint32_t usrCtxSize = it.second.second;
    uint32_t numBlocks = usrCtxSize / blockSize;

    for (uint32_t i = 0; i < numBlocks; i++) {
      CodedBlockMetadata &pbm = mCodedBlockMetadata[base + i];
      // updated for both usr and gc
      if (parent != nullptr) {
        pbm.lba = slot->lbaArray[pos + i];
        if (pbm.lba >= Configuration::GetStorageSpaceInBytes()) {
          debug_error("lba %lx %lu\n", pbm.lba, pbm.lba);
          assert(0);
        } 
      } else {
        pbm.lba = ~0ull;
      }
      pbm.timestamp = slot->timestamp;
    }

    base += numBlocks;
    pos += numBlocks;
  }

  if (base - index < slot->size / blockSize - slot->ioOffset) {
    // parity blocks do not have parent ctx
    isParity = true;
    for (uint32_t i = base - index; i < slot->size / blockSize; i++) {
      CodedBlockMetadata &pbm = mCodedBlockMetadata[base + i];
      pbm.lba = ~0ull;
      pbm.timestamp = slot->timestamp;
    }
  }
  // only need to update the compact stripe table once

  if (mSegmentMeta.useAppend) {
    writeCSTableOldIndex(index, slot->stripeId);
  }

  // base is across n zones
  uint32_t baseOffset = slot->offset + slot->ioOffset;
  for (auto& it : slot->associatedRequests) {
    RequestContext *parent = it.first;
    uint64_t uCtxOffset = it.second.first;
    uint32_t numBlocks = it.second.second / blockSize;

    for (uint32_t i = 0; i < numBlocks; i++) {
      // baseOffset is within a zone
      if (parent != nullptr) {
        FinishBlock(slot->zoneId, baseOffset + i, slot->lbaArray[baseOffset - slot->offset + i]);
      } else {
        FinishBlock(slot->zoneId, baseOffset + i, ~0ull);
      }
    }

    baseOffset += numBlocks;
  }

  assert(baseOffset <= slot->offset + slot->size / blockSize);

  // Finish the padding blocks
  while (baseOffset < slot->offset + slot->size / blockSize) {
    FinishBlock(slot->zoneId, baseOffset, ~0ull);
    baseOffset++;
  }

  // release the user or gc write requests
  slot->associatedRequests.clear();

  // update the write offset
  slot->curOffset = slot->size / blockSize;
}

void Segment::RaiznWriteComplete() {
  if (mOngoingRaiznWrites <= 0) {
    debug_warn("mOngoingRaiznWrites %d\n", mOngoingRaiznWrites);
    return;
//    debug_error("mOngoingRaiznWrites %d\n", mOngoingRaiznWrites);
//    assert(0);
  } 
  mOngoingRaiznWrites--;
}

void Segment::ReadComplete(RequestContext *slot)
{
  uint32_t zoneId = slot->zoneId;
  uint32_t offset = slot->offset;
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t unitSize = mSegmentMeta.stripeUnitSize;
  uint32_t n = mSegmentMeta.n;
  uint32_t k = mSegmentMeta.k;
  RequestContext *parent = nullptr;
  assert(slot->associatedRequests.size() <= 1);
  if (!slot->associatedRequests.empty()) {
    parent = slot->associatedRequests[0].first;
  }

  ReadContext *readContext = slot->associatedRead;
  if (slot->status == READ_REAPING) {
    uint32_t totalContextSize = 0;
    // move all data from stripe context to the user context
    for (auto& it : slot->associatedRequests) {
      auto& parent = it.first;
      auto& uCtxOffset = it.second.first;
      auto& usrCtxSize = it.second.second;
      memcpy((uint8_t *)parent->data + uCtxOffset * blockSize,
             slot->data + totalContextSize, usrCtxSize);
      totalContextSize += usrCtxSize;

      // verification
      if (parent->type != GC) {
        for (int i = 0; i < usrCtxSize / blockSize; i++) {
          // i is in parent offset (start from uCtxOffset)
          uint64_t inSlotOffset = (totalContextSize - usrCtxSize) / blockSize + i;
          // if LBA is ended with 23 1's, it is an index block
          uint64_t lba = (slot->meta) ? ((BlockMetadata*)slot->meta + inSlotOffset)->fields.coded.lba : ~0ull;
          uint64_t currLba = parent->lba + (uCtxOffset + i) * blockSize; 
          uint32_t granu = blockSize * blockSize / 4;
          if ((lba & (granu - 1)) == granu - 1) {
            // index block
            // TODO may fail here. Check
            if (lba / granu != currLba / granu) {
              debug_error("lba %lx %lu currLba %lx %lu - %lu vs %lu\n",
                  lba, lba, currLba, currLba, 
                  lba / granu, currLba / granu); 
              assert(0);
            }
          } else if (currLba != lba) { 
            if (mCodedBlockMetadata != nullptr) {
              CodedBlockMetadata &memPbm = mCodedBlockMetadata[zoneId *
                mDataRegionSize + offset - mHeaderRegionSize + inSlotOffset];
              if (currLba != memPbm.lba) {
                debug_error("in-memory meta: lba wrong %lu vs [%lu]\n",
                    currLba, memPbm.lba);
                assert(0);
              }
            }
            debug_error("read error: lba wrong %lu vs %lu (i=%d)\n", parent->lba + (uCtxOffset + i) * blockSize, lba, i);
            printf("read error: lba wrong %lu vs %lu (i=%d)\n", parent->lba + (uCtxOffset + i) * blockSize, lba, i);
            if (mInMemoryMeta != nullptr) {
              uint64_t index = zoneId * mDataRegionSize + offset - mHeaderRegionSize + inSlotOffset;
              if (memcmp(mInMemoryMeta + index * 64, 
                    slot->meta + inSlotOffset * 64, 64)) {
                char str[200];
                printf("in memory meta: index %lu inSlotOffset %lu "
                    "totalContextSize %u unitSize %u slotSize %u\n",
                    index, inSlotOffset, totalContextSize,
                    unitSize, slot->size); 
                for (int i = 0; i < 64; i++) {
                  int j = i % 16;
                  sprintf(str + j * 3, "%02x ", mInMemoryMeta[index * 64 + i]);
                  if (i % 16 == 15) {
                    printf("%s\n", str);
                  }
                } 
                printf("actual meta: \n");
                for (int i = 0; i < 64; i++) {
                  int j = i % 16;
                  sprintf(str + j * 3, "%02x ", slot->meta[inSlotOffset * 64 + i]);
                  if (i % 16 == 15) {
                    printf("%s\n", str);
                  }
                }
              }
            }
          }
          if (i >= slot->lbaArray.size()) {
            slot->lbaArray.resize(usrCtxSize / blockSize);
          }
          slot->lbaArray[i] = lba;
        }
      }
    }
  } else if (slot->status == DEGRADED_READ_REAPING) {
    bool alive[n];
    for (uint32_t i = 0; i < n; ++i) {
      alive[i] = false;
    }

    for (uint32_t i = 1; i < 1 + k; ++i) {
      uint32_t zid = readContext->ioContext[i]->zoneId;
      alive[zid] = true;
    }

    readContext->data[slot->zoneId] = slot->data;
    if (Configuration::GetSystemMode() == ZONEWRITE_ONLY) {
      uint32_t segStripeId = GetStripeIdFromOffset(offset);
      DecodeStripe(offset, segStripeId, readContext->data, alive, n, k, zoneId, blockSize);
    } else {
      // TODO ZONEAPPEND_ONLY is not handled
      uint32_t groupSize = mSegmentMeta.stripeGroupSize;
      // slot->stripeId comes from ReadStripe()
      uint32_t offsetOfStripe = mHeaderRegionSize + 
          round_down(offset - mHeaderRegionSize, groupSize * unitSize / blockSize) +
          slot->stripeId * unitSize / blockSize;
      uint32_t segStripeId = GetStripeIdFromOffset(offsetOfStripe);
      DecodeStripe(offsetOfStripe, segStripeId, readContext->data, alive, n, k, zoneId, blockSize);
    }
  }

  if (!Configuration::GetDeviceSupportMetadata()) {
    for (int i = 0; i < slot->size / blockSize; i++) {
      BlockMetadata *meta = (BlockMetadata*)slot->meta + i;
      BlockMetadata *parentMeta = (BlockMetadata*)parent->meta + i;
      uint32_t index = zoneId * mDataRegionSize + offset - mHeaderRegionSize + i;
      uint32_t stripeId = (mSegmentMeta.useAppend) ? readCSTableOldIndex(index) : 1;
      meta->fields.coded.lba = mCodedBlockMetadata[index].lba;
      meta->fields.coded.timestamp = mCodedBlockMetadata[index].timestamp;
      meta->fields.replicated.stripeId = stripeId; 
      parentMeta->fields.coded.lba = mCodedBlockMetadata[index].lba;
      parentMeta->fields.coded.timestamp = mCodedBlockMetadata[index].timestamp;
      parentMeta->fields.replicated.stripeId = stripeId; 
    }
  }

  // We assume that there is only one associated request for GC reads
  if (parent->type == GC && parent->meta) {
    for (int i = 0; i < slot->size / blockSize; i++) {
      BlockMetadata *result = (BlockMetadata*)slot->meta + i;
      BlockMetadata *parentMeta = (BlockMetadata*)parent->meta + i;

      if (mCodedBlockMetadata != nullptr) {
        uint64_t codedLba = 0, codedTimestamp = 0;
        CodedBlockMetadata &pbm = mCodedBlockMetadata[zoneId *
          mDataRegionSize + offset - mHeaderRegionSize + i];
        codedLba = pbm.lba;
        codedTimestamp = pbm.timestamp;

        if (codedLba != result->fields.coded.lba) {
          // TODO: this is a hack for trace-driven experiments
          debug_error("GC read error: lba wrong %lu vs %lu (i=%d)\n", codedLba, result->fields.coded.lba, i);
          printf("GC read error: lba wrong %lu vs %lu (i=%d)\n", codedLba, result->fields.coded.lba, i);
          result->fields.coded.lba = codedLba;
          result->fields.coded.timestamp = codedTimestamp;
        }
      }

      parentMeta->fields.coded.lba = parent->lbaArray[i] = result->fields.coded.lba;
      parentMeta->fields.coded.timestamp = result->fields.coded.timestamp;
      parentMeta->fields.replicated.stripeId = result->fields.replicated.stripeId;
    }
  }
}

// lock issue: This function is called by index thread. 
// But FinishBlock() and WriteComplete() is called by dispatch thread
void Segment::InvalidateBlock(uint32_t zoneId, uint32_t realOffset)
{
  std::scoped_lock<std::shared_mutex> w_lock(mLock);
  assert(mNumInvalidBlocks < mNumBlocks);
  uint64_t base = zoneId * mDataRegionSize + realOffset - mHeaderRegionSize;

  if ((mValidBits[base / 8] & (1 << (base % 8))) == 0) {
    // will invalidate the block soon in FinishBlock
    // the reason is that the block may be invalid soon after written
    mToBeInvalid.insert(base);
  } else {
    mNumInvalidBlocks += 1;
    mValidBits[base / 8] &= (255 - (1 << (base % 8)));
  }
}

void Segment::FinishBlock(uint32_t zoneId, uint32_t offset, uint64_t lba)
{
  std::scoped_lock<std::shared_mutex> w_lock(mLock);
  uint64_t base = zoneId * mDataRegionSize + offset - mHeaderRegionSize;
  if (lba != ~0ull) {
    mNumBlocks += 1;
    if (lba % 4096 > 0) {
      mNumIndexBlocks += 1;
    }
    if (mToBeInvalid.find(base) != mToBeInvalid.end()) {
      // invalidate the block stored in the buffer
      mNumInvalidBlocks += 1;
      mToBeInvalid.erase(base);
      mValidBits[base / 8] &= (255 - (1 << (base % 8)));
    } else {
      mValidBits[base / 8] |= (1 << (base % 8));
    }
  } else {
    mValidBits[base / 8] &= (255 - (1 << (base % 8)));
  }
}

void Segment::ReleaseZones()
{
  for (auto zone : mZones) {
    zone->Release();
  }
}

bool Segment::CheckOutstandingWrite()
{
  recycleStripeWriteContexts();
  return !mStripeWriteContextPool->NoInflightStripes();
}

uint32_t Segment::GetSegmentId()
{
  return mSegmentMeta.segmentId;
}

uint32_t Segment::GetStripeUnitSize() {
  return mSegmentMeta.stripeUnitSize;
}

uint64_t Segment::GetCapacity() const
{
  return mDataRegionSize;
}

void Segment::ReclaimReadContext(ReadContext *readContext)
{
  mReadContextPool->ReturnContext(readContext);
}

// For recovery
void Segment::SetSegmentStatus(SegmentStatus status)
{
  mSegmentStatus = status;
}

void Segment::RecoverLoadAllBlocks()
{
  uint32_t numZones = mZonesWpForRecovery.size();
  uint32_t unitSize = mSegmentMeta.stripeUnitSize;
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t metadataSize = Configuration::GetMetadataSize();

  for (uint32_t i = 0; i < numZones; ++i) {
    mDataBufferForRecovery[i] = (uint8_t*)spdk_zmalloc(
        mDataRegionSize * blockSize, 4096,
        NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    if (mDataBufferForRecovery[i] == nullptr) {
      debug_error("Error in allocating memory for recovery. errno %d %s."
          " Try to set the number of huge pages with sysctl command\n", errno,
          strerror(errno));
      assert(0);
    }
    mMetadataBufferForRecovery[i] = (uint8_t*)spdk_zmalloc(
        mDataRegionSize * metadataSize, metadataSize,
        NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    assert(mDataBufferForRecovery[i] != nullptr);
    assert(mMetadataBufferForRecovery[i] != nullptr);
  }

  assert(mDataBufferForRecovery != nullptr);
  assert(mMetadataBufferForRecovery != nullptr);

  printf("Read blocks from zones\n");
  uint32_t offsets[numZones];
  for (uint32_t i = 0; i < numZones; ++i) {
    offsets[i] = mHeaderRegionSize;
  }

  while (true) {
    uint32_t counter = 0;
    for (uint32_t i = 0; i < numZones; ++i) {
      uint64_t wp = mZonesWpForRecovery[i].first;
      uint64_t zslba = mZones[i]->GetSlba();
      uint32_t wpInZone = wp - zslba;
      uint32_t offsetInDataRegion = offsets[i] - mHeaderRegionSize;
      uint32_t numBlocks2Fetch = std::min(wpInZone - offsets[i], 32u);

      if (numBlocks2Fetch > 0) {
        // update compact stripe table
        int error = 0;
        if ((error = spdk_nvme_ns_cmd_read_with_md(
            mZones[i]->GetDevice()->GetNamespace(),
            mZones[i]->GetDevice()->GetIoQueue(0),
            mDataBufferForRecovery[i] + offsetInDataRegion * blockSize,
            mMetadataBufferForRecovery[i] + offsetInDataRegion * metadataSize,
            zslba + offsets[i], numBlocks2Fetch,
            completeOneEvent, &counter, 0, 0, 0)) < 0) {
          printf("Error in reading %d %s.\n", error, strerror(error));
        }
        offsets[i] += numBlocks2Fetch;
        counter += 1;
      }
    }

    if (counter == 0) {
      break;
    }

    while (counter != 0) {
      for (uint32_t i = 0; i < numZones; ++i) {
        spdk_nvme_qpair_process_completions(mZones[i]->GetDevice()->GetIoQueue(0), 0);
      }
    }
  }
  printf("Finish read all blocks.\n");

  for (uint32_t i = 0; i < numZones; ++i) {
    uint64_t wp = mZonesWpForRecovery[i].first;
    uint64_t zslba = mZones[i]->GetSlba();
    for (uint32_t j = 0; j < wp - zslba - mHeaderRegionSize; ++j) {
      uint32_t index = i * mDataRegionSize + j;
      BlockMetadata *blockMeta = &(((BlockMetadata*)mMetadataBufferForRecovery[i])[j]);
      uint64_t lba = blockMeta->fields.coded.lba;
      uint64_t writeTimestamp = blockMeta->fields.coded.timestamp;
      uint16_t stripeId = blockMeta->fields.replicated.stripeId;
      uint32_t stripeOffset = ~0u;

      if (Configuration::GetSystemMode() == ZONEWRITE_ONLY) {
        stripeOffset = mHeaderRegionSize + j;
      } else {
        uint32_t groupSize = mSegmentMeta.stripeGroupSize;
        stripeOffset = mHeaderRegionSize + 
          round_down(j, groupSize * unitSize / blockSize) + 
          stripeId * unitSize / blockSize;
      }

      uint32_t globalStripeId = GetStripeIdFromOffset(stripeOffset);

      if (i == Configuration::CalculateDiskId(
            globalStripeId, mZones.size() - 1, 
            mSegmentMeta.raidScheme, mZones.size())) {
        // parity
        mCodedBlockMetadata[index].lba = ~0ull;
        mCodedBlockMetadata[index].timestamp = 0;
      } else {
        mCodedBlockMetadata[index].lba = blockMeta->fields.coded.lba;
        mCodedBlockMetadata[index].timestamp = blockMeta->fields.coded.timestamp;
      }

//      if (Configuration::GetSystemMode() == ZAPRAID) 
      if (mSegmentMeta.useAppend) {
        writeCSTableOldIndex(index, stripeId);
      }
    }
  }
}

bool Segment::RecoverFooterRegionIfNeeded()
{
  bool needed = false;
  uint32_t numZones = mZonesWpForRecovery.size();
  uint32_t blockSize = Configuration::GetBlockSize();
  for (uint32_t i = 0; i < numZones; ++i) {
    Zone *zone = mZones[i];
    uint64_t zslba = zone->GetSlba();
    uint64_t wp = mZonesWpForRecovery[i].first - zslba;

    if (wp >= mHeaderRegionSize + mDataRegionSize) {
      needed = true;
    }
  }

  if (!needed) {
    return false;
  }

  uint8_t *footerBlock = (uint8_t*)spdk_zmalloc(Configuration::GetBlockSize(), Configuration::GetBlockSize(),
                                      NULL, SPDK_ENV_SOCKET_ID_ANY,
                                      SPDK_MALLOC_DMA);
  uint32_t offsets[numZones];
  for (uint32_t i = 0; i < numZones; ++i) {
    offsets[i] = mZonesWpForRecovery[i].first - mZones[i]->GetSlba();
  }

  for (uint32_t i = 0; i < numZones; ++i) {
    Zone *zone = mZones[i];
    uint64_t zslba = zone->GetSlba();

    for (uint32_t pos = offsets[i]; pos < mHeaderRegionSize + mDataRegionSize + mFooterRegionSize; ++pos) {
      uint32_t begin = (pos - 1 - mHeaderRegionSize - mDataRegionSize) * (blockSize / 20);
      uint32_t end = std::min(mDataRegionSize, begin + blockSize / 20);

      uint32_t base = i * mDataRegionSize;
      uint32_t posInBlk = 0;
      bool done = false;
      for (uint32_t offset = begin; offset < end; ++offset) {
        *(uint64_t*)(footerBlock + (offset - begin) * 20 + 0) = mCodedBlockMetadata[base + offset].lba;
        *(uint64_t*)(footerBlock + (offset - begin) * 20 + 8) = mCodedBlockMetadata[base + offset].timestamp;

        uint32_t stripeId = (mSegmentMeta.useAppend) ? 
          readCSTableOldIndex(base + offset) : 1;
        *(uint32_t*)(footerBlock + (offset - begin) * 20 +16) = stripeId;
      }

      // same as synchronous write
      if (spdk_nvme_ns_cmd_write(zone->GetDevice()->GetNamespace(),
          zone->GetDevice()->GetIoQueue(0),
          footerBlock, pos, 1, complete, &done, 0) != 0) {
        fprintf(stderr, "Write error in recovering footer region.\n");
      }
      while (!done) {
        spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
      }
    }

    bool done = false;
    if (spdk_nvme_zns_finish_zone(zone->GetDevice()->GetNamespace(),
          zone->GetDevice()->GetIoQueue(0),
          zslba, 0, complete, &done) != 0) {
      fprintf(stderr, "Seal error in recovering footer region.\n");
    }
    while (!done) {
      spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
    }
  }

  spdk_free(footerBlock);
  return true;
}

void Segment::RecoverIndexFromSealedSegment(
    uint8_t *buffer, std::pair<uint64_t, PhysicalAddr> *indexMap)
{
  uint32_t numZones = mZones.size();
  for (uint32_t zid = 0; zid < numZones; ++zid) {
    Zone *zone = mZones[zid];
    for (uint32_t offset = 0; offset < mFooterRegionSize; offset += 512) {
      bool done = false;
      spdk_nvme_ns_cmd_read(zone->GetDevice()->GetNamespace(),
          zone->GetDevice()->GetIoQueue(0),
          buffer + offset * Configuration::GetBlockSize(),
          zone->GetSlba() + mHeaderRegionSize + mDataRegionSize + offset,
          std::min(512u, mFooterRegionSize - offset), complete, &done, 0);
      while (!done) {
        spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
      }
    }

    uint32_t blockSize = Configuration::GetBlockSize();
    for (uint32_t offsetInFooterRegion = 0; offsetInFooterRegion < mFooterRegionSize; ++offsetInFooterRegion) {
      uint32_t begin = offsetInFooterRegion * (blockSize / 20);
      uint32_t end = std::min(mDataRegionSize, begin + blockSize / 20);

      for (uint32_t offsetInDataRegion = begin; offsetInDataRegion < end; ++offsetInDataRegion) {
        uint8_t *metadataForCurrentBlock = buffer + offsetInFooterRegion * blockSize + (offsetInDataRegion - begin) * 20;
        uint64_t lba = *(uint64_t*)(metadataForCurrentBlock + 0);
        uint64_t writeTimestamp = *(uint64_t*)(metadataForCurrentBlock + 8);
        uint32_t stripeId = *(uint32_t*)(metadataForCurrentBlock + 16);

        if (mSegmentMeta.useAppend) {
          // update compact stripe table first
          uint32_t index = zid * mDataRegionSize + offsetInDataRegion;
          writeCSTableOldIndex(index, stripeId);
        }

        PhysicalAddr pba;
        pba.segment = this;
        pba.zoneId = zid;
        pba.offset = mHeaderRegionSize + offsetInDataRegion;

        if (lba == ~0ull) {
          continue;
        }

        if (lba >= Configuration::GetStorageSpaceInBytes()) {
          debug_error("read LBA %lu or %lx from meta, error\n", lba, lba);
          assert(0);
        }

        if (indexMap[lba / blockSize].second.segment == nullptr
            || writeTimestamp > indexMap[lba / blockSize].first) {
          indexMap[lba / blockSize] = std::make_pair(writeTimestamp, pba);
        }
      }
    }
  }

  mPos = mHeaderRegionSize + mDataRegionSize + mFooterRegionSize;

  delete []mAdminStripe->data;
  delete []mAdminStripe->metadata;
  delete mAdminStripe;
  // release the in-memory block metadata
  if (Configuration::GetDeviceSupportMetadata()) {
    delete []mCodedBlockMetadata;
    mCodedBlockMetadata = nullptr;
  }
}

void Segment::RecoverIndexFromOpenSegment(
    std::pair<uint64_t, PhysicalAddr> *indexMap)
{
  uint32_t blockSize = Configuration::GetBlockSize();
  // The block must have been brought into the memory and
  // mCodedBlockMetadata is revived accordingly
  for (uint32_t zid = 0; zid < mZones.size(); ++zid) {
    for (uint32_t offset = 0; offset < mPos; ++offset) {
      uint32_t index = zid * mDataRegionSize + offset;
      uint64_t lba = mCodedBlockMetadata[index].lba;
      uint64_t writeTimestamp = mCodedBlockMetadata[index].timestamp;

      PhysicalAddr pba;
      pba.segment = this;
      pba.zoneId = zid;
      pba.offset = mHeaderRegionSize + offset;
      
      if (lba == ~0ull) {
        continue;
      }
      if (indexMap[lba / blockSize].second.segment == nullptr
          || writeTimestamp > indexMap[lba / blockSize].first) {
        indexMap[lba / blockSize] = std::make_pair(writeTimestamp, pba);
      }
    }
  }
}

bool Segment::RecoverNeedRewrite()
{
  uint32_t unitSize = mSegmentMeta.stripeUnitSize;
  uint32_t blockSize = Configuration::GetBlockSize();
  // stripe ID to blocks
  bool needRewrite = false;
  if (Configuration::GetSystemMode() == ZAPRAID) {
    uint32_t checkpoint = ~0u;
    for (uint32_t i = 0; i < mZonesWpForRecovery.size(); ++i) {
      Zone *zone = mZones[i];
      uint64_t zslba = zone->GetSlba();
      uint32_t wp = mZonesWpForRecovery[i].first - zslba;

      uint32_t begin = round_down(wp - mHeaderRegionSize,
        mSegmentMeta.stripeGroupSize * unitSize / blockSize);
      uint32_t end = begin + 
        mSegmentMeta.stripeGroupSize * unitSize / blockSize;
      if (checkpoint == ~0) {
        checkpoint = begin;
      } else {
        if (checkpoint != begin) {
          printf("Error in the checkpoint during need rewrite.\n");
        }
      }

      for (uint32_t offset = begin; offset < end; ++offset) {
        uint32_t index = i * mDataRegionSize + offset;
        uint32_t stripeId = (mSegmentMeta.useAppend) ?
          readCSTableOldIndex(index) : 1;
        if (mStripesToRecover.find(stripeId) == mStripesToRecover.end()) {
          mStripesToRecover[stripeId].resize(mZones.size());
          for (uint32_t j = 0; j < mZones.size(); ++j) {
            mStripesToRecover[stripeId][j] = ~0u;
          }
        }

        // can have multiple offsets. Use the smaller offset
        if (mStripesToRecover[stripeId][i] == ~0u ||
          mStripesToRecover[stripeId][i] > offset) {
          mStripesToRecover[stripeId][i] = offset;
          assert(offset % (unitSize / blockSize) == 0);
        }
      }
    }

    for (auto stripe : mStripesToRecover) {
      uint32_t stripeId = stripe.first;
      const auto &blocks = stripe.second;

      for (uint32_t i = 0; i < mZones.size(); ++i) {
        if (blocks[i] == ~0u) {
          needRewrite = true;
        }
      }
    }
  } else {
    uint32_t agreedWp = 0;
    for (uint32_t i = 0; i < mZonesWpForRecovery.size(); ++i) {
      Zone *zone = mZones[i];
      uint64_t zslba = zone->GetSlba();
      uint32_t wp = mZonesWpForRecovery[i].first - zslba;
      if (agreedWp == 0) {
        agreedWp = wp;
      }

      if (agreedWp != wp) {
        needRewrite = true;
      }
    }
  }

  return needRewrite;
}

void Segment::RecoverState()
{
  mPos = mZonesWpForRecovery[0].first - mZones[0]->GetSlba();

  if (mPos == mHeaderRegionSize + mDataRegionSize) {
    mSegmentStatus = SEGMENT_PREPARE_FOOTER;
  } else {
    mSegmentStatus = SEGMENT_NORMAL;
  }
}

void Segment::RecoverFromOldSegment(Segment *oldSegment)
{
  uint32_t unitSize = mSegmentMeta.stripeUnitSize;
  uint32_t blockSize = Configuration::GetBlockSize();
  // First, write the header region
  uint32_t counter = mZones.size();
  RequestContext *contexts[mZones.size()];
  for (uint32_t i = 0; i < mZones.size(); ++i) {
    Zone *zone = mZones[i];
    contexts[i] = mRequestContextPool->GetRequestContext(false);
    memcpy(contexts[i]->dataBuffer, &mSegmentMeta, sizeof(mSegmentMeta));

    if (spdk_nvme_ns_cmd_write_with_md(
            zone->GetDevice()->GetNamespace(),
            zone->GetDevice()->GetIoQueue(0),
            contexts[i]->dataBuffer, contexts[i]->metadataBuffer,
            zone->GetSlba(), 1, completeOneEvent, &counter, 0, 0, 0) != 0) {
      fprintf(stderr, "Write error in writing the header for new segment.\n");
    }
  }

  while (counter != 0) {
    for (uint32_t i = 0; i < mZones.size(); ++i) {
      Zone *zone = mZones[i];
      spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
    }
  }

  // Then, rewrite all valid blocks
  uint32_t checkpoint = ~0u;
  if (Configuration::GetSystemMode() == ZAPRAID) {
    for (uint32_t i = 0; i < oldSegment->mZonesWpForRecovery.size(); ++i) {
      Zone *zone = oldSegment->mZones[i];
      uint64_t zslba = zone->GetSlba();
      uint32_t wp = oldSegment->mZonesWpForRecovery[i].first - zslba;
      uint32_t lastCheckpoint = round_down(wp - mHeaderRegionSize,
          mSegmentMeta.stripeGroupSize * unitSize / blockSize);

      if (checkpoint == ~0) {
        checkpoint = lastCheckpoint;
      } else if (checkpoint != lastCheckpoint) {
        printf("Critical error: checkpoint not match.\n");
      }
    }
  } else if (Configuration::GetSystemMode() == ZONEWRITE_ONLY) {
    for (uint32_t i = 0; i < oldSegment->mZonesWpForRecovery.size(); ++i) {
      Zone *zone = oldSegment->mZones[i];
      uint64_t zslba = zone->GetSlba();
      uint32_t wp = oldSegment->mZonesWpForRecovery[i].first - zslba;
      checkpoint = std::min(wp, checkpoint);
    }
    // A fix in the new version. The checkpoint should not include the header 
    checkpoint -= mHeaderRegionSize;
  }
  // Blocks (both data and parity) before the checkpoint can be kept as is.
  for (uint32_t i = 0; i < checkpoint; ++i) {
    struct timeval s, e;
    gettimeofday(&s, NULL);
    uint32_t counter = mZones.size();
    for (uint32_t j = 0; j < mZones.size(); ++j) {
      Zone *zone = mZones[j];
      // TODO should be ok
      memcpy(contexts[j]->dataBuffer, oldSegment->mDataBufferForRecovery[j] + i * Configuration::GetBlockSize(), 4096);
      memcpy(contexts[j]->metadataBuffer, oldSegment->mMetadataBufferForRecovery[j] + i * Configuration::GetMetadataSize(), 4096);
      if (spdk_nvme_ns_cmd_write_with_md(
            zone->GetDevice()->GetNamespace(),
            zone->GetDevice()->GetIoQueue(0),
            contexts[j]->dataBuffer, contexts[j]->metadataBuffer,
            zone->GetSlba() + mHeaderRegionSize + i, 1, completeOneEvent, &counter, 0, 0, 0) != 0) {
        fprintf(stderr, "Write error in writing the previous groups for new segment.\n");
      }
      uint32_t index = j * mDataRegionSize + i;
      mCodedBlockMetadata[index].lba = oldSegment->mCodedBlockMetadata[index].lba;
      mCodedBlockMetadata[index].timestamp = oldSegment->mCodedBlockMetadata[index].timestamp;
      if (mSegmentMeta.stripeGroupSize <= 256) {
        mCompactStripeTable[index] = oldSegment->mCompactStripeTable[index];
      } else if (mSegmentMeta.stripeGroupSize <= 65536) {
        ((uint16_t*)mCompactStripeTable)[index] = oldSegment->mCompactStripeTable[index];
      } else {
        uint8_t *tmp = mCompactStripeTable + index * 3;
        uint8_t *oldStripeId = oldSegment->mCompactStripeTable + index * 3;
        *(tmp + 2) = *(oldStripeId + 2);
        *(uint16_t*)tmp = *(uint16_t*)(oldStripeId);
      }
    }
    while (counter != 0) {
      for (uint32_t i = 0; i < mZones.size(); ++i) {
        Zone *zone = mZones[i];
        spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
      }
    }
    gettimeofday(&e, NULL);
    double elapsed = e.tv_sec - s.tv_sec + e.tv_usec / 1000000. - s.tv_usec / 1000000.;
  }

  // For blocks in the last group
  uint32_t maxStripeId = 0;
  uint32_t numZones = mZones.size();
  for (auto stripe : oldSegment->mStripesToRecover) {
    // the iteration order must be from the smallest to the largest;
    // so filter out invalid stripe and re-assign the stripe IDs will not
    // affect the ordering
    const auto &blocks = stripe.second;
    uint32_t counter = mZones.size();

    bool valid = true;
    for (uint32_t i = 0; i < mZones.size(); ++i) {
      if (blocks[i] == ~0u) {
        valid = false;
      }
    }

    if (!valid) {
      continue;
    }

    for (uint32_t i = 0; i < numZones; ++i) {
      uint32_t index = i * mDataRegionSize + checkpoint + maxStripeId;
      BlockMetadata *blockMeta = reinterpret_cast<BlockMetadata*>(
          oldSegment->mMetadataBufferForRecovery[i] + blocks[i] * Configuration::GetMetadataSize());

      // TODO check here
      // maxStripeId is the new stripe ID for the next stripe
      uint32_t stripeId = GetStripeIdFromOffset(mHeaderRegionSize + checkpoint
          + maxStripeId * unitSize / blockSize);

      if (i == Configuration::CalculateDiskId(
            stripeId, mZones.size() - 1,
            mSegmentMeta.raidScheme, mZones.size()
            //mHeaderRegionSize + index, mZones.size() - 1,
            //mSegmentMeta.raidScheme, mZones.size()
            )
         ) {
        // parity
        mCodedBlockMetadata[index].lba = ~0ull;
        mCodedBlockMetadata[index].timestamp = 0;
      } else {
        mCodedBlockMetadata[index].lba = blockMeta->fields.coded.lba;
        mCodedBlockMetadata[index].timestamp = blockMeta->fields.coded.timestamp;
      }

      blockMeta->fields.replicated.stripeId = maxStripeId;
      if (mSegmentMeta.useAppend) {
        writeCSTableOldIndex(index, maxStripeId);
      }

      // use zone write to recover
      if (spdk_nvme_ns_cmd_write_with_md(
            mZones[i]->GetDevice()->GetNamespace(),
            mZones[i]->GetDevice()->GetIoQueue(0),
            oldSegment->mDataBufferForRecovery[i] + blocks[i] * Configuration::GetBlockSize(),
            oldSegment->mMetadataBufferForRecovery[i] + blocks[i] * Configuration::GetMetadataSize(),
            mZones[i]->GetSlba() + mHeaderRegionSize + checkpoint + maxStripeId * unitSize / blockSize,
            1, completeOneEvent, &counter, 0, 0, 0) != 0) {
        fprintf(stderr, "Write error in rewriting the last group in the new segment.\n");
      }
    }
    while (counter != 0) {
      for (uint32_t i = 0; i < mZones.size(); ++i) {
        Zone *zone = mZones[i];
        spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
      }
    }
    maxStripeId += 1;
  }

  mPos = mHeaderRegionSize + checkpoint + maxStripeId * unitSize / blockSize;
  if (mPos == mHeaderRegionSize + mDataRegionSize) {
    mSegmentStatus = SEGMENT_PREPARE_FOOTER;
  } else {
    mSegmentStatus = SEGMENT_NORMAL;
  }
}

void Segment::SetZonesAndWpForRecovery(std::vector<std::pair<uint64_t, uint8_t*>> zonesWp)
{
  mZonesWpForRecovery = zonesWp;
}

bool Segment::isUsingAppend() {
  return mSegmentMeta.useAppend;
}

void Segment::ResetInRecovery()
{
  for (uint32_t i = 0; i < mZones.size(); ++i) {
    Zone *zone = mZones[i];
    bool done = false;
    if (spdk_nvme_zns_reset_zone(zone->GetDevice()->GetNamespace(),
          zone->GetDevice()->GetIoQueue(0),
          zone->GetSlba(), 0, complete, &done) != 0) {
      fprintf(stderr, "Reset error in recovering.\n");
    }
    while (!done)
    {
      spdk_nvme_qpair_process_completions(zone->GetDevice()->GetIoQueue(0), 0);
    }
    zone->GetDevice()->ReturnZone(zone);
  }
}

void Segment::FinishRecovery()
{
  // 
  uint32_t numBlocks = 0;
  if (mSegmentStatus == SEGMENT_SEALED) {
    numBlocks = mDataRegionSize * mSegmentMeta.k;
  } else {
    numBlocks = (mPos - mHeaderRegionSize) * mSegmentMeta.k;
  }

  mNumInvalidBlocks = numBlocks - mNumBlocks;
  mNumBlocks = numBlocks;
  return;
}


uint32_t Segment::GetPos() {
  return mPos;
}

bool Segment::HasInFlightStripe() {
  return mPosInStripe != 0 || mCurSlot != nullptr; 
}

std::map<uint32_t, uint32_t> Segment::RetrieveAppendLatencies() {
  return mAppendLatencies;
}

std::map<uint32_t, uint32_t> Segment::RetrieveParityLatencies() {
  return mStripeStartWriteLatencies[mSegmentMeta.k];
}

void Segment::Dump() {
  printf("Segment ID: %lu, n: %u, k: %u, numZones: %u, raidScheme: %u, position: %u, stripe offset: %u\n",
      mSegmentMeta.segmentId,
      mSegmentMeta.n,
      mSegmentMeta.k,
      mSegmentMeta.numZones,
      mSegmentMeta.raidScheme,
      mPos, mPosInStripe);
  printf("\tStripe Group Size: %u, unit size: %u, use append: %u\n",
      mSegmentMeta.stripeGroupSize, mSegmentMeta.stripeUnitSize,
      mSegmentMeta.useAppend);
  printf("\tZones: ");
  for (uint32_t i = 0; i < mSegmentMeta.numZones; ++i) {
    printf("%lu ", mSegmentMeta.zones[i]);
  }
  printf("\n\tnumBlocks: %u, numInvalidBlocks: %u\n", mNumBlocks, mNumInvalidBlocks);
  uint64_t total_cnt = 0, total_time = 0;
  for (auto& it : mAppendLatencies) {
    if (it.first > 4000000) continue;
    total_cnt += it.second;
    total_time += (uint64_t)it.first * it.second;
  }
  printf("Append avg: %.2f\n", (double)total_time / total_cnt);

  total_cnt = 0, total_time = 0;
  for (auto& it : mAppendHalfLatencies) {
    if (it.first > 4000000) continue;
    total_cnt += it.second;
    total_time += (uint64_t)it.first * it.second;
  }
  printf("Read stripe prepare avg: %.2f\n", (double)total_time / total_cnt);

  total_cnt = 0, total_time = 0;

//  DumpLatencies(mStripeLatencies, "write stripe");
//  for (int i = 0; i < 4; i++) {
//    std::string str = "stripe start_" + std::to_string(i);
//    DumpLatencies(mStripeStartWriteLatencies[i], str.c_str());
//  }
}

SegmentMetadata Segment::DumpSegmentMeta() {
  return mSegmentMeta;
}
