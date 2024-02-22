#include "device.h"

#include <rte_errno.h>
#include <sys/time.h>
#include "raid_controller.h"
#include "zone.h"
#include "messages_and_functions.h"

#include "spdk/nvme.h"

// callbacks for io completions, in IO threads
static void writeComplete(void *arg, const struct spdk_nvme_cpl *completion)
{
  struct timeval s, e;
  gettimeofday(&s, 0);
  RequestContext *slot = (RequestContext*)arg;

  if (spdk_nvme_cpl_is_error(completion)) {
    slot->PrintStats();
    fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "Write I/O failed, aborting run. zone %u offset %u size %u"
        " io.offset %lu io.size %u\n",
        slot->zoneId, slot->offset, slot->size, 
        slot->ioContext.offset, slot->ioContext.size);
    assert(0);
    exit(1);
  }

  if (Configuration::GetEnableIOLatencyTest()) {
    gettimeofday(&slot->timeB, 0);
  }

  uint32_t blockSize = Configuration::GetBlockSize();
  slot->successBytes += slot->ioContext.size * blockSize;
  slot->zone->AdvancePos(slot->ioContext.size);
  StatsRecorder::getInstance()->timeProcess(StatsType::WRITE_LAT, slot->timeA);

  debug_warn("write complete %d slot %p offset %u size %u slot->zone->pos %u\n",
      slot->successBytes, slot, slot->offset, slot->size, slot->zone->GetPos());
  if (slot->status != WRITE_REAPING) {
    debug_error("slot %p status %d\n", slot, slot->status);
  }
  assert(slot->status == WRITE_REAPING);
  debug_w("queue");
  gettimeofday(&e, 0);
  slot->ctrl->MarkIoThreadZoneWriteCompleteTime(s, e);
  slot->Queue();
};

static void readComplete(void *arg, const struct spdk_nvme_cpl *completion)
{
  RequestContext *slot = (RequestContext*)arg;
  if (spdk_nvme_cpl_is_error(completion)) {
    slot->PrintStats();
    fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "Read I/O failed, aborting run\n");
    exit(1);
  }

  // TODO change to unit size
//  slot->successBytes += Configuration::GetBlockSize();
  if (Configuration::GetEnableIOLatencyTest()) {
    gettimeofday(&slot->timeB, 0);
  }
  uint32_t blockSize = Configuration::GetBlockSize();
  slot->successBytes += slot->ioContext.size * blockSize;
  debug_warn("read complete %d slot %p\n", slot->successBytes, slot);
  assert(slot->status == READ_REAPING);
  slot->Queue();
  debug_w("queue");
};

static void resetComplete(void *arg, const struct spdk_nvme_cpl *completion)
{
  RequestContext *slot = (RequestContext*)arg;
  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "Reset I/O failed, aborting run\n");
    exit(1);
  }

  debug_warn("reset complete %d slot %p\n", slot->successBytes, slot);
  assert(slot->status == RESET_REAPING);
  slot->Queue();
};


static void finishComplete(void *arg, const struct spdk_nvme_cpl *completion)
{
  RequestContext *slot = (RequestContext*)arg;
  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "Finish I/O failed, aborting run\n");
    exit(1);
  }
  debug_warn("finish complete %d slot %p\n", slot->successBytes, slot);
  assert(slot->status == FINISH_REAPING);
  slot->Queue();
};

static void appendComplete(void *arg, const struct spdk_nvme_cpl *completion)
{
  RequestContext *slot = (RequestContext*)arg;
  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "Append I/O failed, aborting run\n");
    exit(1);
  }

  if (Configuration::GetEnableIOLatencyTest()) {
    gettimeofday(&slot->timeB, 0);
  }

  uint32_t blockSize = Configuration::GetBlockSize();
  slot->successBytes += slot->ioContext.size * blockSize;
  slot->zone->AdvancePos(slot->ioContext.size);

  slot->offset = slot->offset & completion->cdw0;
  debug_warn("append complete %d slot %p\n", slot->successBytes, slot);
  assert(slot->status == WRITE_REAPING);
  slot->Queue();
};

inline uint64_t Device::bytes2Block(uint64_t bytes)
{
  return bytes / Configuration::GetBlockSize(); //>> 12;
}

inline uint64_t Device::bytes2ZoneNum(uint64_t bytes)
{
  return bytes2Block(bytes) / mZoneSize;
}

void Device::Init(struct spdk_nvme_ctrlr *ctrlr, int nsid)
{
  mController = ctrlr;
  mNamespace = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
  if (spdk_nvme_ns_get_md_size(mNamespace) == 0) {
    Configuration::SetDeviceSupportMetadata(false);
  }

  mZoneSize = spdk_nvme_zns_ns_get_zone_size_sectors(mNamespace);
  mNumZones = spdk_nvme_zns_ns_get_num_zones(mNamespace);
  if (mZoneSize == 2ull * 1024 * 256) { 
    mZoneCapacity = 1077 * 256; // hard-coded here since it is ZN540; update this for emulated SSDs
  } else {
    mZoneCapacity = mZoneSize;
  }
  printf("Zone size: %lu, zone cap: %lu, num of zones: %u\n", mZoneSize, mZoneCapacity, mNumZones);

  struct spdk_nvme_io_qpair_opts opts;
  spdk_nvme_ctrlr_get_default_io_qpair_opts(mController, &opts, sizeof(opts));
  opts.delay_cmd_submit = true;
  opts.create_only = true;
  mIoQueues = new struct spdk_nvme_qpair*[Configuration::GetNumIoThreads()];
  for (int i = 0; i < Configuration::GetNumIoThreads(); ++i) {
    mIoQueues[i] = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, &opts, sizeof(opts));
    assert(mIoQueues[i]);
  }
  mReadCounts.clear();
  mTotalReadCounts = 0;
}

void Device::ConnectIoPairs()
{
  for (int i = 0; i < Configuration::GetNumIoThreads(); ++i) {
    if (spdk_nvme_ctrlr_connect_io_qpair(mController, mIoQueues[i]) < 0) {
      printf("Connect ctrl failed!\n");
    }
  }
}

void Device::EraseWholeDevice()
{
  bool done = false;
  auto resetComplete = [](void *arg, const struct spdk_nvme_cpl *completion) {
    bool *done = (bool*)arg;
    *done = true;
  };

  spdk_nvme_zns_reset_zone(mNamespace, mIoQueues[0], 0, true, resetComplete, &done);

  while (!done) {
    spdk_nvme_qpair_process_completions(mIoQueues[0], 0);
  }
}

void Device::InitZones(uint32_t numNeededZones, uint32_t numReservedZones)
{
  if (numNeededZones + numReservedZones > mNumZones) {
    printf("Warning! The real storage space is not sufficient for the setting,"
        "%u %u %u\n", numNeededZones, numReservedZones, mNumZones);
  }
  mNumZones = std::min(mNumZones, numNeededZones + numReservedZones);
  mZones = new Zone[mNumZones];
  for (int i = 0; i < mNumZones; ++i) {
    mZones[i].Init(this, i * mZoneSize, mZoneCapacity, mZoneSize);
    mAvailableZones.insert(&mZones[i]);
  }
}

bool Device::HasAvailableZone()
{
  return !mAvailableZones.empty();
}

Zone* Device::OpenZone()
{
  assert(!mAvailableZones.empty());
  auto it = mAvailableZones.begin();
  Zone* zone = *it;
  mAvailableZones.erase(it);

  return zone;
}

Zone* Device::OpenZoneBySlba(uint64_t slba)
{
  uint32_t zid = slba / mZoneSize;
  Zone *zone = &mZones[zid];

  assert(mAvailableZones.find(zone) != mAvailableZones.end());
  mAvailableZones.erase(zone);

  return zone;
}

void Device::ReturnZone(Zone* zone)
{
  mAvailableZones.insert(zone);
}

void Device::issueIo2(spdk_event_fn event_fn, RequestContext *slot)
{
  static uint32_t ioThreadId = 0;
  slot->ioContext.ns = mNamespace;
  slot->ioContext.qpair = mIoQueues[ioThreadId];
  event_call(Configuration::GetIoThreadCoreId(ioThreadId), event_fn, slot, nullptr);
  ioThreadId = (ioThreadId + 1) % Configuration::GetNumIoThreads();
}

void Device::issueIo(spdk_msg_fn msg_fn, RequestContext *slot)
{
  static uint32_t ioThreadId = 0;
  slot->ioContext.ns = mNamespace;
  slot->ioContext.qpair = mIoQueues[ioThreadId];
  thread_send_msg(slot->ctrl->GetIoThread(ioThreadId), msg_fn, slot);
  ioThreadId = (ioThreadId + 1) % Configuration::GetNumIoThreads();
}

void Device::ResetZone(Zone* zone, void *ctx)
{
  RequestContext *slot = (RequestContext*)ctx;
  slot->ioContext.cb = resetComplete;
  slot->ioContext.ctx = ctx;
  slot->ioContext.offset = zone->GetSlba();
  slot->ioContext.flags = 0;

  if (Configuration::GetEventFrameworkEnabled()) {
    issueIo2(zoneReset2, slot);
  } else {
    issueIo(zoneReset, slot);
  }
}

void Device::FinishZone(Zone *zone, void *ctx)
{
  RequestContext *slot = (RequestContext*)ctx;
  slot->ioContext.cb = finishComplete;
  slot->ioContext.ctx = ctx;
  slot->ioContext.offset = zone->GetSlba();
  slot->ioContext.flags = 0;

  if (Configuration::GetBypassDevice()) {
    debug_e("Bypass finish zone");
    slot->Queue();
    return;
  }

  if (Configuration::GetEventFrameworkEnabled()) {
    issueIo2(zoneFinish2, slot);
  } else {
    issueIo(zoneFinish, slot);
  }
}

void Device::Write(uint64_t offset, uint32_t size, void* ctx)
{
  RequestContext *slot = (RequestContext*)ctx;
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t metadataSize = Configuration::GetMetadataSize();
  slot->ioContext.data = (uint8_t*)slot->data + slot->curOffset * blockSize;
  slot->ioContext.metadata = (uint8_t*)slot->meta + slot->curOffset * metadataSize;
  slot->ioContext.offset = bytes2Block(offset);
  slot->ioContext.size = bytes2Block(size);  
  slot->ioContext.cb = writeComplete;
  slot->ioContext.ctx = ctx;
  slot->ioContext.flags = 0;

  slot->ioOffset = slot->curOffset;

  if (Configuration::GetBypassDevice()) {
    debug_e("Bypass write zone");
    slot->successBytes += size;
    slot->Queue();
    return;
  }

//  if (Configuration::GetEventFrameworkEnabled()) {
//    issueIo2(zoneWrite2, slot);
//  } else {
//    issueIo(zoneWrite, slot);
//  }

  BufferCopyArgs* bcArgs = new BufferCopyArgs;
  bcArgs->dev = this;
  bcArgs->slot = slot;
  if (Configuration::GetEventFrameworkEnabled()) {
    event_call(Configuration::GetCompletionThreadCoreId(), bufferCopy2, 
        bcArgs, nullptr);
  } else {
    thread_send_msg(slot->ctrl->GetCompletionThread(), bufferCopy, bcArgs);
  }
}

void Device::Append(uint64_t offset, uint32_t size, void* ctx)
{
  RequestContext *slot = (RequestContext*)ctx;
  slot->ioContext.data = slot->data + slot->curOffset * Configuration::GetBlockSize();
  slot->ioContext.metadata = slot->meta + slot->curOffset * Configuration::GetMetadataSize();
  slot->ioContext.offset = bytes2Block(offset);
  slot->ioContext.size = bytes2Block(size); 
  slot->ioContext.cb = appendComplete;
  slot->ioContext.ctx = ctx;
  slot->ioContext.flags = 0;

  slot->ioOffset = slot->curOffset;

  if (Configuration::GetBypassDevice()) {
    debug_e("Bypass append zone");
    slot->offset = 0;
    slot->successBytes += Configuration::GetBlockSize();
    slot->Queue();
    return;
  }

  BufferCopyArgs* bcArgs = new BufferCopyArgs;
  bcArgs->dev = this;
  bcArgs->slot = slot;
  if (Configuration::GetEventFrameworkEnabled()) {
    event_call(Configuration::GetCompletionThreadCoreId(), bufferCopy2, 
        bcArgs, nullptr);
  } else {
    thread_send_msg(slot->ctrl->GetCompletionThread(), bufferCopy, bcArgs);
  }
//  if (Configuration::GetEventFrameworkEnabled()) {
//    issueIo2(zoneAppend2, slot);
//  } else {
//    issueIo(zoneAppend, slot);
//  }
}

void Device::Read(uint64_t offset, uint32_t size, void* ctx)
{
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t metadataSize = Configuration::GetMetadataSize();
  RequestContext *slot = (RequestContext*)ctx;
  slot->ioContext.data = slot->data + slot->curOffset * blockSize;
  slot->ioContext.metadata = slot->meta + slot->curOffset * metadataSize;
  slot->ioContext.offset = bytes2Block(offset);
  slot->ioContext.size = bytes2Block(size); 
  slot->ioContext.cb = readComplete;
  slot->ioContext.ctx = ctx;
  slot->ioContext.flags = 0;

  slot->ioOffset = slot->curOffset;

  {
    mReadCounts[slot->ioContext.size]++;
    mTotalReadCounts++;
    mTotalReadSizes += size;
    if (mTotalReadCounts == 100000 || mTotalReadCounts % 1000000 == 0) {
      printf("Read counts: ");
      for (auto it = mReadCounts.begin(); it != mReadCounts.end(); ++it) {
        printf("%u: %lu, ", it->first, it->second);
      }
      printf("\n");
      printf("Read sizes: %lu\n", mTotalReadSizes);
    }
  }

  if (Configuration::GetBypassDevice()) {
    debug_e("Bypass read zone");
    slot->successBytes += Configuration::GetBlockSize();
    slot->Queue();
    return;
  }

  if (Configuration::GetEventFrameworkEnabled()) {
    issueIo2(zoneRead2, slot);
  } else {
    issueIo(zoneRead, slot);
  }
}

void Device::AddAvailableZone(Zone *zone)
{
  mAvailableZones.insert(zone);
}

uint64_t Device::GetZoneCapacity()
{
  return mZoneCapacity;
}

uint64_t Device::GetZoneSize() 
{
  return mZoneSize;
}

uint32_t Device::GetNumZones()
{
  return mNumZones;
}

void b();
void Device::ReadZoneHeaders(std::map<uint64_t, uint8_t*> &zones)
{
  bool done = false;
  auto complete = [](void *arg, const struct spdk_nvme_cpl *completion) {
    bool *done = (bool*)arg;
    *done = true;
  };

  // Read zone report
  uint32_t nr_zones = spdk_nvme_zns_ns_get_num_zones(mNamespace);
  struct spdk_nvme_zns_zone_report *report;
  uint32_t report_bytes = sizeof(report->descs[0]) * nr_zones + sizeof(*report);
  report = (struct spdk_nvme_zns_zone_report *)calloc(1, report_bytes);
  spdk_nvme_zns_report_zones(mNamespace, mIoQueues[0], report, report_bytes, 0,
                             SPDK_NVME_ZRA_LIST_ALL, false, complete, &done);
  while (!done) {
    spdk_nvme_qpair_process_completions(mIoQueues[0], 0);
  }

  for (uint32_t i = 0; i < report->nr_zones; ++i) {
    struct spdk_nvme_zns_zone_desc *zdesc = &(report->descs[i]);
    uint64_t wp = ~0ull;
    uint64_t zslba = zdesc->zslba;

    if (zdesc->zs == SPDK_NVME_ZONE_STATE_FULL
        || zdesc->zs == SPDK_NVME_ZONE_STATE_IOPEN 
        || zdesc->zs == SPDK_NVME_ZONE_STATE_EOPEN) {
      if (zdesc->wp != zslba) {
        wp = zdesc->wp;
      }
    }
    
    if (wp == ~0ull) {
      continue;
    }

    uint8_t *buffer = (uint8_t*)spdk_zmalloc(Configuration::GetBlockSize(), 4096,
                                   NULL, SPDK_ENV_SOCKET_ID_ANY,
                                   SPDK_MALLOC_DMA);
    done = false;
    spdk_nvme_ns_cmd_read(mNamespace, mIoQueues[0], buffer, zslba, 1, complete, &done, 0);
    while (!done) {
      spdk_nvme_qpair_process_completions(mIoQueues[0], 0);
    }

    zones[wp] = buffer;
  }

  free(report);
}

void Device::SetDeviceTransportAddress(const char *addr) {
  memcpy(mTransportAddress, addr, SPDK_NVMF_TRADDR_MAX_LEN + 1);
}

char* Device::GetDeviceTransportAddress() const
{
  return (char*)mTransportAddress;
}
