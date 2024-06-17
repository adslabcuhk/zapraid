#ifndef __RAID_CONTROLLER_H__
#define __RAID_CONTROLLER_H__

#include <atomic>
#include <vector>
#include <queue>
#include <list>
#include <unordered_map>
#include <unordered_set>
#include "common.h"
#include "device.h"
#include "indexmap.h"
#include "segment.h"
#include "spdk/thread.h"
#include "stats_recorder.h"

class Segment;
class IndexMap;

class RAIDController {
public:
  ~RAIDController();
  /**
   * @brief Initialzie the RAID block device
   * 
   * @param need_env need to initialize SPDK environment. Example of false,
   *                 app is part of a SPDK bdev and the bdev already initialized the environment.
   */
  void Init(bool need_env);

  /**
   * @brief Write a block from the RAID system
   * 
   * @param offset the logical block address of the RAID block device, in bytes, aligned with 4KiB
   * @param size the number of bytes written, in bytes, aligned with 4KiB
   * @param data the data buffer
   * @param cb_fn call back function provided by the client (SPDK bdev)
   * @param cb_args arguments provided to the call back function
   */
  void Write(uint64_t offset, uint32_t size, void *data, zns_raid_request_complete cb_fn, void *cb_args);

  /**
   * @brief Read a block from the RAID system
   * 
   * @param offset the logical block address of the RAID block device, in bytes, aligned with 4KiB
   * @param size the number of bytes read, in bytes, aligned with 4KiB
   * @param data the data buffer
   * @param cb_fn call back function provided by the client (SPDK bdev)
   * @param cb_args arguments provided to the call back function
   */
  void Read(uint64_t offset, uint32_t size, void *data, zns_raid_request_complete cb_fn, void *cb_args);

  void Execute(uint64_t offset, uint32_t size, void* data, bool is_write,
      zns_raid_request_complete cb_fn, void *cb_args);
  void ExecuteIndex(RequestContext *ctx);

  void EnqueueWrite(RequestContext *ctx);
  void EnqueueReadPrepare(RequestContext *ctx);
  void EnqueueReadReaping(RequestContext *ctx);
  void EnqueueWriteIndex(RequestContext *ctx);
  void EnqueueReadIndex(RequestContext *ctx);
  void EnqueueWaitIndex(RequestContext *usrCtx, uint64_t lba);
  void RemoveWaitIndex(RequestContext *usrCtx, uint64_t lba);
  uint64_t GetWaitIndexSize();
  void PrintWaitIndexTime(double startTime);
  std::queue<RequestContext*>& GetWriteQueue();
  std::queue<RequestContext*>& GetReadPrepareQueue();
  std::queue<RequestContext*>& GetReadReapingQueue();
  std::queue<RequestContext*>& GetWriteIndexQueue();
  std::queue<RequestContext*>& GetReadIndexQueue();
  uint64_t rescheduleIndexTask();

  // added for zone-write parallelism
  void EnqueueZoneWrite(RequestContext *ctx);
//  std::map<uint32_t, std::map<uint32_t, RequestContext*>>& GetPendingZoneWriteQueue();
  std::map<Zone*, std::map<uint32_t, RequestContext*>>& GetPendingZoneWriteQueue();

  std::queue<RequestContext*>& GetEventsToDispatch();


  /**
   * @brief Drain the RAID system to finish all the in-flight requests
   */
  void Drain();

  /**
   * @brief Get the Request Queue object
   * 
   * @return std::queue<RequestContext*>& 
   */
  std::queue<RequestContext*>& GetRequestQueue();
  std::mutex& GetRequestQueueMutex();

  void UpdateIndexNeedLock(uint64_t lba, PhysicalAddr phyAddr);
  IndexMapStatus UpdateIndex(uint64_t lba, PhysicalAddr phyAddr);
  int GetNumInflightRequests();
  bool ProceedGc();
  bool ExistsGc();
  bool CheckSegments();

  void WriteInDispatchThread(RequestContext *ctx);
  void ReadInDispatchThread(RequestContext *ctx);
  void EnqueueEvent(RequestContext *ctx);

  // debug
  void CompleteWrite();
  void CompleteRead(); 
  void RetrieveWRCounts(uint64_t& sw, uint64_t& sr, uint64_t& cw, uint64_t& cr); 
  void RetrieveIndexWRCounts(uint64_t& sw, uint64_t& sr, uint64_t& cw, uint64_t& cr); 

  uint32_t GcBatchUpdateIndex(const std::vector<uint64_t> &lbas, const std::vector<std::pair<PhysicalAddr, PhysicalAddr>> &pbas);

  /**
   * @brief Get the Io Thread object
   * 
   * @param id 
   * @return struct spdk_thread* 
   */
  struct spdk_thread *GetIoThread(int id);
  /**
   * @brief Get the Dispatch Thread object
   * 
   * @return struct spdk_thread* 
   */
  struct spdk_thread *GetDispatchThread();
  /**
   * @brief Get the Ec Thread object
   * 
   * @return struct spdk_thread* 
   */
  struct spdk_thread *GetEcThread();
  /**
   * @brief Get the Index And Completion Thread object
   * 
   * @return struct spdk_thread* 
   */
  struct spdk_thread *GetIndexThread();

  /**
   * @brief Get the Completion Thread object
   * 
   * @return struct spdk_thread* 
   */
  struct spdk_thread *GetCompletionThread();

  /**
   * @brief Find a PhysicalAddress given a LogicalAddress of a block
   * 
   * @param lba the logial address of the queried block
   * @param phyAddr the pointer to store the physical address of the queried block
   * @return true the queried block was written before and not trimmed
   * @return false the queried block was not written before or was trimmed
   */
  IndexMapStatus LookupIndex(uint64_t lba, PhysicalAddr *phyAddr);

  void ProgressGcIndexUpdate();

  void ReclaimContexts();
  void ReclaimIndexContexts();
  void Flush();
  void Dump();
  void PrintSegmentPos();

  uint32_t GetHeaderRegionSize();
  uint32_t GetDataRegionSize();
  uint32_t GetFooterRegionSize();
  uint32_t GetZoneSize();
  bool isLargeRequest(uint32_t size); 

  GcTask* GetGcTask();
  void RemoveRequestFromGcEpochIfNecessary(RequestContext *ctx);
  void MarkWriteLatency(RequestContext* ctx);
  void MarkReadLatency(RequestContext* ctx);

  FILE* GetIndexMappingFd() { return mIndexMappingFileFd; }

  void SetEventPoller(spdk_poller* p) {mEventsPoller = p;}
  void SetBackgroundPoller(spdk_poller* p) {mBackgroundPoller = p;}

  // for index map
  RequestContext* PrepareIndexContext(IndexMapStatus s, uint64_t lba);
  void HandleIndexContext(RequestContext* ctx); 

  void MarkCompletionThreadBufferCopyTime(struct timeval s, struct timeval e);
  void MarkCompletionThreadIdleTime(struct timeval s, struct timeval e);
  void MarkIoThreadZoneWriteTime(struct timeval s, struct timeval e);
  void MarkIoThreadZoneWriteCompleteTime(struct timeval s, struct timeval e);
  void MarkIoThreadCheckPendingWritesTime(struct timeval s, struct timeval e);
  void MarkDispatchThreadEnqueueTime(struct timeval s, struct timeval e);
  void MarkDispatchThreadHandleContextTime(struct timeval s, struct timeval e);
  void MarkDispatchThreadEventTime(struct timeval s, struct timeval e);
  void MarkDispatchThreadBackgroundTime(struct timeval s, struct timeval e);
  void MarkDispatchThreadTestTime(struct timeval s, struct timeval e);
  void MarkIndexThreadUpdatePbaTime(struct timeval s, struct timeval e);
  void MarkIndexThreadQueryPbaTime(struct timeval s, struct timeval e);
  void printCompletionThreadTime();
  void printIoThreadTime(double t);
  void printDispatchThreadTime(double t);
  void printIndexThreadTime(double t);

private:
  RequestContext* getContextForUserRequest();
  RequestContext* getContextForIndex();
  void doWrite(RequestContext *context);
  void doRead(RequestContext *context);
  
  void initEcThread();
  void initDispatchThread();
  void initIoThread();
  void initIndexThread();
  void initCompletionThread();
  void initGc();

  void createSegmentIfNeeded(Segment **segment, uint32_t spId);
  bool scheduleGc();

  void initializeGcTask();
  bool progressGcWriter();
  bool progressGcReader();

  void restart();
  void rebuild(uint32_t failedDriveId);

  void GenerateSegmentWriteOrder(uint32_t size, int* res, int& id);

  std::vector<Device*> mDevices;
  std::unordered_set<Segment*> mSealedSegments;
  std::vector<Segment*> mSegmentsToSeal;
  std::vector<Segment*> mOpenSegments;
  Segment* mSpareSegment;
//  PhysicalAddr *mAddressMap;
  IndexMap *mAddressMap;
  uint32_t *mAddressMapMemory;
  std::map<uint64_t, uint64_t> mOrigMap;

  uint32_t mN = 0;
  uint32_t mTotalNumSegments = 0;
  RequestContextPool *mRequestContextPoolForUserRequests;
  std::unordered_set<RequestContext*> mInflightUserRequestContext;
  std::unordered_set<RequestContext*> mInflightIndexRequestContext;

  RequestContextPool *mRequestContextPoolForSegments;
  RequestContextPool *mRequestContextPoolForIndex;
  ReadContextPool *mReadContextPool;
  StripeWriteContextPool **mStripeWriteContextPools;

  std::queue<RequestContext*> mRequestQueue;
  std::mutex mRequestQueueMutex;

  struct GcTask mGcTask;

  uint32_t mNumOpenSegments = 1;
  uint64_t mNumCurrentOpenSegments = 0;
  uint64_t mNumOpenSegmentsThres = 14;
  std::map<uint32_t, bool> mNotOpenForFullOpenZones;

  IoThread mIoThread[16];
  struct spdk_thread *mDispatchThread;
  struct spdk_thread *mEcThread;
  struct spdk_thread *mIndexThread;
  struct spdk_thread *mCompletionThread;

  std::queue<RequestContext*> mEventsToDispatch;
  std::queue<RequestContext*> mWriteQueue;
  std::queue<RequestContext*> mReadPrepareQueue;
  std::queue<RequestContext*> mReadReapingQueue;
  std::queue<RequestContext*> mWriteIndexQueue;
  std::queue<RequestContext*> mReadIndexQueue;
  // user requests. The mappings from LBA unit to user requests 
  std::map<uint64_t, std::set<RequestContext*>> mWaitIndexQueue; 
  std::map<double, uint64_t> mWaitIndexTimeToLba;
  std::map<uint64_t, double> mWaitIndexLbaToTime;

  // debug
  std::map<RequestContext*, double> mWaitIndexToTime;

//  std::map<uint32_t, std::map<uint32_t, RequestContext*>> mIoZoneWriteMap;
  std::map<Zone*, std::map<uint32_t, RequestContext*>> mIoZoneWriteMap;

  // Segment table. only touched by dispatch thread
  std::shared_mutex mSegmentTableMutex;
  Segment** mZoneToSegmentMap;
  std::map<uint32_t, std::vector<uint32_t>> mSegIdToZoneIdMap;

  uint32_t mAvailableStorageSpaceInSegments = 0;
  uint32_t mStorageSpaceThresholdForGcInSegments = 0;
  uint32_t mNumTotalZones = 0;

  uint32_t mNextAssignedSegmentId = 0;
  uint32_t mGlobalTimestamp = 0;

  uint32_t mHeaderRegionSize = 0;
  uint32_t mDataRegionSize = 0;
  uint32_t mFooterRegionSize = 0;
  uint32_t mZoneSize = 0;
  uint32_t mMappingBlockUnitSize = 0;

  // differentiating small and large-chunk segments
  bool *mOpenGroupForLarge;

  spdk_poller* mEventsPoller = nullptr;
  spdk_poller* mBackgroundPoller = nullptr;
  std::vector<spdk_poller*> mIoPollers;
  std::vector<spdk_poller*> mPendingZoneWritePollers;

  // stats
  std::map<uint32_t, uint64_t> mSpIdWriteSizes;
  std::map<uint32_t, uint32_t> mWriteLatCnt;
  std::map<uint32_t, uint32_t> mReadLatCnt;

  // debug
  double mTimeInUs = 0;
  int mPrintTime = 0;
  std::atomic<uint64_t> mStartedWrites = 0, mStartedReads = 0;
  std::atomic<uint64_t> mCompletedWrites = 0, mCompletedReads = 0;
  FILE* mIndexMappingFileFd = nullptr;
  uint64_t mCompletionThreadBufferCopyTime = 0; 
  uint64_t mCompletionThreadIdleTime = 0; 
  uint64_t mIoThreadZoneWriteTime = 0;
  uint64_t mIoThreadZoneWriteCompleteTime = 0;
  uint64_t mIoThreadCheckPendingWritesTime = 0;
  uint64_t mDispatchThreadHandleContextTime = 0;
  uint64_t mDispatchThreadEventTime = 0;
  uint64_t mDispatchThreadBackgroundTime = 0;
  uint64_t mDispatchThreadEnqueueTime = 0;
  uint64_t mDispatchThreadTestTime = 0;
  uint64_t mIndexThreadUpdatePbaTime = 0;
  uint64_t mIndexThreadQueryPbaTime = 0;
  std::atomic<uint64_t> mNumIndexWrites = 0;
  std::atomic<uint64_t> mNumIndexReads = 0;
  std::atomic<uint64_t> mNumIndexWritesHandled = 0;
  std::atomic<uint64_t> mNumIndexReadsHandled = 0;

  // for RAIZN: metadata zones organization
  std::vector<std::list<Zone*>> mMetazones;
  // meta zones whose remaining in-flight writes can fill the zone
  // i.e., meta zones whose WillBeFull() returns true
  std::map<uint32_t, Zone*> mWritingMetaZones;
  // meta zones that are reseting 
  std::map<uint32_t, Zone*> mResettingMetaZones;
  std::vector<RequestContext> mResetMetaZoneContexts;

  std::unordered_set<RequestContext*> mReadsInCurrentGcEpoch;
};

#endif
