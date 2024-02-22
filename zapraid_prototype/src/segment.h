#ifndef __ZONE_GROUP_H__
#define __ZONE_GROUP_H__
#include "common.h"
#include "zone.h"
#include <set>
#include <vector>
#include <list>
#include <shared_mutex>

class RAIDController;

// Write at the beginning of each zone in the segment; replicate to each zone
struct SegmentMetadata {
  uint64_t segmentId; // 8
  uint64_t zones[16]; // 128
  uint64_t metazones[16]; // 128
  uint32_t stripeSize; // 16384
  uint32_t stripeDataSize; // 12288 
  uint32_t stripeParitySize; // 4096
  uint32_t stripeGroupSize = 256; // 256 or 1
  uint32_t stripeUnitSize;
  uint32_t n; // 4
  uint32_t k; // 3 
  uint32_t numZones; // 4
  RAIDLevel raidScheme; // 1
  uint8_t  useAppend;  // 0 for Zone Write and 1 for Zone Append
};

enum SegmentStatus {
  SEGMENT_NORMAL,
  SEGMENT_CONCLUDING_WRITES_IN_GROUP,
  SEGMENT_CONCLUDING_APPENDS_IN_GROUP,
  SEGMENT_WRITING_HEADER,
  SEGMENT_PREPARE_FOOTER,
  SEGMENT_WRITING_FOOTER,
  SEGMENT_SEALING,
  SEGMENT_SEALED,
  SEGMENT_METAZONE_PREPARE_RESET,
  SEGMENT_METAZONE_RESETTING
};

enum AppendStatus {
};

class Segment
{
public:
  Segment(RAIDController* raidController, uint32_t segmentId,
          RequestContextPool *ctxPool, ReadContextPool *rctxPool,
          StripeWriteContextPool *sctxPool, uint32_t configurationId = 0);
  Segment(RAIDController* raidController, 
          RequestContextPool *ctxPool, ReadContextPool *rctxPool,
          StripeWriteContextPool *sctxPool, SegmentMetadata* segMeta);
  void initSegment(RAIDController* raidController, 
          RequestContextPool *ctxPool, ReadContextPool *rctxPool,
          StripeWriteContextPool *sctxPool, uint32_t segmentId, int stripeSize, 
          int n, int k, uint8_t useAppend, uint32_t stripeGroupSize);
  ~Segment();

  /**
   * @brief Add a zone to the segment.
   *        Called by controller when creating new segment.
   * 
   * @param zone 
   */
  void AddZone(Zone *zone);

  void AddMetaZone(Zone *zone);

  /**
   * @brief Finalize the new segment creation
   *
   */
  void FinalizeCreation();
  /**
   * @brief Summarize the header and write the headers to the zones.
   *        Called by controller after intializing all zones.
   * 
   */
  void FinalizeSegmentHeader();

  /**
   * @brief Get the Zones object
   * 
   * @return const std::vector<Zone*>& 
   */
  const std::vector<Zone*>& GetZones();

  /**
   * @brief (Attempt) to append a block to the segment. The user requests
   * related to Append() need to wait until the whole stripe is written.
   * It is called by BufferedAppend() under ZONEWRITE_ONLY, ZONEAPPEND_ONLY,
   * and ZAPRAID.
   * 
   * @return >0 the size successfully issued the block to the drive
   * @return 0 the segment is busy; cannot proceed to issue
   */
  uint32_t Append();

  /**
   * @brief (Attempt) to partially Append a block to the segment. Only for
   * RAIZN_SIMPLE when the function is called, it will also write a parity
   * update to a metadata zone. It is called by BufferedAppend() under
   * RAIZN_SIMPLE.
   * 
   * @return >0 the size successfully issued the block to the drive (excluding the metadata)
   */
  uint32_t PartialAppend(bool hasPpu);

  /**
   * @brief (Attempt) to Append a block to the segment
   * 
   * @param ctx the user request that contains the data, lba, etc
   * @param offset which block in the user request to append
   * @return >0 the size successfully issued the block to the drive
   * @return 0 the segment is busy; cannot proceed to issue
   */
  uint32_t BufferedAppend(RequestContext *ctx, uint32_t offset, uint32_t size);

  /**
   * @brief Read a block from the segment
   * 
   * @param ctx the user request that contains the data buffer, lba, etc
   * @param offset which block in the user request to append
   * @param phyAddr the physical block address of the requested block
   * @param size the number of bytes for reads
   * @return true successfully issued the request to the drive
   * @return false the segment is busy (no read context remains); cannot proceed to read
   */
  bool Read(RequestContext *ctx, uint32_t offset, PhysicalAddr phyAddr, uint32_t size);

  /**
   * @brief Ensure the block is valid before performing read
   * 
   */
  bool ReadValid(RequestContext *ctx, uint32_t offset, PhysicalAddr phyAddr, uint32_t size);

  /**
   * @brief Reset all the zones in the segment
   *        Called by controller only after the segment is collected by GC
   * 
   * @param ctx the controller maintained context, tracking the reset progress
   */
  void Reset(RequestContext *ctx);

  /**
   * @brief Reset all the meta zones in the segment 
   */
  void ResetMetaZones();

  bool IsResetDone();

  /**
   * @brief Seal the segment by finishing all the zones
   *        Called by controller after the data region is full and the footer region is persisted
   */
  void Seal();

  uint64_t GetCapacity() const;
  uint32_t GetNumBlocks() const;
  uint32_t GetNumInvalidBlocks() const;
  uint32_t GetFullNumBlocks() const;
  uint32_t GetSegmentId();
  uint32_t GetStripeUnitSize();
  uint32_t GetZoneSize();

  /**
   * @brief Specify whether the segment can accept new blocks
   * 
   * @return true The segment is full and cannot accept new blocks; need to write the footer
   * @return false The segment is not full and can accept new blocks
   */
  bool IsFull();
  bool CanSeal();
  uint32_t GetStripeIdFromOffset(uint32_t offset);

  bool CheckOutstandingWrite();
  bool CheckOutstandingRead();

//  void ReadStripeMeta(RequestContext *ctx);
  void ReadStripe(RequestContext *ctx);

  void WriteComplete(RequestContext *ctx);
  void ReadComplete(RequestContext *ctx);

  void InvalidateBlock(uint32_t zoneId, uint32_t realOffset);
  void FinishBlock(uint32_t zoneId, uint32_t realOffset, uint64_t lba);
  void PrintStats();
  void Dump();

  std::map<uint32_t, uint32_t> RetrieveAppendLatencies(); 
  std::map<uint32_t, uint32_t> RetrieveParityLatencies(); 
  
  // Added for multiple stripe support 
  SegmentMetadata DumpSegmentMeta();

  void ReclaimReadContext(ReadContext *context);

  void FlushCurrentStripe();
  bool StateTransition();
  SegmentStatus GetStatus();
  void ReleaseZones();

  void GenerateParityBlock(StripeWriteContext *stripe, uint32_t zonePos,
      uint32_t stripeId, struct timeval, bool useMetaZone);
  void ProgressFooterWriter();

  uint32_t GetPos();
  bool HasInFlightStripe();

  // For recovery
  void SetSegmentStatus(SegmentStatus status);
  
  void SetZonesAndWpForRecovery(std::vector<std::pair<uint64_t, uint8_t*>> zonesWp);
  bool isUsingAppend(); 

  void RecoverLoadAllBlocks();
  bool RecoverFooterRegionIfNeeded();
  bool RecoverNeedRewrite();
  void RecoverFromOldSegment(Segment *segment);
  void RecoverState();

  void RecoverIndexFromSealedSegment(uint8_t *buffer, std::pair<uint64_t, PhysicalAddr> *indexMap);
  void RecoverIndexFromOpenSegment(std::pair<uint64_t, PhysicalAddr> *indexMap);

  void ResetInRecovery();
  void FinishRecovery();

  // debug
  void Invalidate() {mInvalidFlag = true;}
  bool isInvalid() {return mInvalidFlag;}

  bool isForGc() {return mSetAsGcFlag;}
  void setForGc() { mSetAsGcFlag = true;}
  void unsetForGc() { mSetAsGcFlag = false;}

private:
  RequestContextPool *mRequestContextPool;

  StripeWriteContext *mCurStripe;

  ReadContextPool *mReadContextPool;
  StripeWriteContextPool *mStripeWriteContextPool;

  void degradedRead(RequestContext *ctx, PhysicalAddr phyAddr);

  void recycleStripeWriteContexts();
  void recycleReadContexts();
  RequestContext* GetRequestContext();
  void ReturnRequestContext(RequestContext *slot);
  bool checkStripeAvailable(StripeWriteContext *stripeContext);
  bool checkReadAvailable(ReadContext *stripeContext);
  
  void writeCSTableOldIndex(uint32_t index, uint32_t stripeId);
  uint32_t readCSTableOldIndex(uint32_t index);
  void writeCSTable(uint32_t index, uint32_t stripeId);
  uint32_t readCSTable(uint32_t index);
  bool findStripe();
  void encodeStripe(uint8_t **stripe, uint32_t n, uint32_t k, uint32_t unitSize);
  void decodeStripe(uint32_t offset, uint8_t **stripe, bool *alive, uint32_t n, uint32_t k, uint32_t decodeIndex);

  void fillInFooterBlock(uint8_t **data, uint32_t pos);

  // 10ms for those selected for GC, which is 100us of the normal timeout
  uint32_t getSyncTimeoutInUs() { return (mSetAsGcFlag ? 10000 : 100);}
  
//  bool* mValidBits;
  uint8_t* mValidBits;
  std::set<uint64_t> mToBeInvalid;
  uint64_t mvblength;
  std::shared_mutex mLock;
  uint8_t* mCompactStripeTable = nullptr;
  CodedBlockMetadata *mCodedBlockMetadata;

  std::vector<Zone*> mZones;
  std::vector<Zone*> mMetaZones;
  std::vector<RequestContext> mResetContext;

  // position in **number of blocks**
  // from 0 to zone_capacity / block_size
  uint32_t mPos;

  // the number of blocks in the header
  uint32_t mHeaderRegionSize;
  // the number of blcoks in the data region
  uint32_t mDataRegionSize;
  // the number of blocks in the footer
  uint32_t mFooterRegionSize;

  uint32_t mNumInvalidBlocks;
  uint32_t mNumBlocks;
  uint32_t mNumIndexBlocks;

  // position in **bytes**
  // from 0 to n * unit_size / block_size
  uint32_t mPosInStripe;
  // the global stripe ID
  uint32_t mCurStripeId;

  RequestContext* mCurSlot = nullptr;

  static uint8_t *gEncodeMatrix;
  static uint8_t *gGfTables;

  struct timeval mS, mE;

  SegmentMetadata mSegmentMeta;
  SegmentStatus mSegmentStatus;

  RAIDController *mRaidController;

  StripeWriteContext *mAdminStripe;

  bool mSetAsGcFlag = false;

  // for debug; performance breakdown
  struct timeval mTimeval;
  struct timeval mTimevalBetweenAppend;
  struct timeval mTimevalStripe;
  std::map<uint32_t, uint32_t> mAppendLatencies;
  std::map<uint32_t, uint32_t> mAppendHalfLatencies;
  std::map<uint32_t, uint32_t> mParityLatencies;
  std::map<uint32_t, uint32_t> mStripeLatencies;
  std::map<uint32_t, uint32_t> mStripeStartWriteLatencies[8];
  bool mFlag = false;
  bool mInvalidFlag = false;
  uint8_t* mInMemoryMeta = nullptr;

  // for recovery
  uint8_t *mDataBufferForRecovery[16];
  uint8_t *mMetadataBufferForRecovery[16];
  std::map<uint32_t, std::vector<uint64_t>> mStripesToRecover;
  std::vector<std::pair<uint64_t, uint8_t*>> mZonesWpForRecovery;
  double mLastStripeCreationTimestamp = 0;
  double mLastAppendTimestamp = 0;

  // for raizn, write the metadata zone
  // meta zone positions
  uint32_t mMetaZonePos[16];
  uint32_t mMetaZoneResets; 
  uint32_t mOngoingMetaWrites = 0;
};

#endif

