#include "raid_controller.h"

#include <sys/time.h>
#include <algorithm>
#include <spdk/env.h>
#include <spdk/nvme.h>
#include <spdk/rpc.h>
#include <spdk/event.h>
#include <spdk/init.h>
#include <spdk/string.h>
#include <isa-l.h>
#include <rte_mempool.h>
#include <rte_errno.h>
#include <thread>

#include <algorithm>

#include "debug.h"
#include "zone.h"
#include "messages_and_functions.h"

static void busyWait(bool *ready)
{
  while (!*ready) {
    if (spdk_get_thread() == nullptr) {
      std::this_thread::sleep_for(std::chrono::seconds(0));
    }
  }
}

static std::vector<Device*> g_devices;

static auto probe_cb = [](void *cb_ctx,
    const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr_opts *opts) -> bool {
  return true;
};

static auto attach_cb = [](void *cb_ctx,
    const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr *ctrlr,
    const struct spdk_nvme_ctrlr_opts *opts) -> void {

  for (int nsid = 1; nsid <= 1; nsid++) {
    Device* device = new Device();
    device->Init(ctrlr, nsid);
    device->SetDeviceTransportAddress(trid->traddr);
    g_devices.emplace_back(device);
  }

  return;
};

static auto quit(void *args)
{
  exit(0);
}

static auto unregister_poller(spdk_poller** ppoller) {
  spdk_poller_unregister(ppoller);
}

void RAIDController::initEcThread()
{
  struct spdk_cpuset cpumask;
  spdk_cpuset_zero(&cpumask);
  spdk_cpuset_set_cpu(&cpumask, Configuration::GetEcThreadCoreId(), true);
  mEcThread = spdk_thread_create("ECThread", &cpumask);
  printf("Create EC processing thread %s %lu\n",
      spdk_thread_get_name(mEcThread),
      spdk_thread_get_id(mEcThread));
  int rc = spdk_env_thread_launch_pinned(Configuration::GetEcThreadCoreId(), ecWorker, this);
  if (rc < 0) {
    printf("Failed to launch ec thread error: %s\n", spdk_strerror(rc));
  }
}

void RAIDController::initIndexThread()
{
  struct spdk_cpuset cpumask;
  spdk_cpuset_zero(&cpumask);
  spdk_cpuset_set_cpu(&cpumask, Configuration::GetIndexThreadCoreId(), true);
  mIndexThread = spdk_thread_create("IndexThread", &cpumask);
  printf("Create index and completion thread %s %lu\n",
         spdk_thread_get_name(mIndexThread),
         spdk_thread_get_id(mIndexThread));
  int rc = spdk_env_thread_launch_pinned(Configuration::GetIndexThreadCoreId(), indexWorker, this);
  if (rc < 0) {
    printf("Failed to launch index completion thread, error: %s\n", spdk_strerror(rc));
  }
}

void RAIDController::initCompletionThread()
{
  struct spdk_cpuset cpumask;
  spdk_cpuset_zero(&cpumask);
  spdk_cpuset_set_cpu(&cpumask, Configuration::GetCompletionThreadCoreId(), true);
  mCompletionThread = spdk_thread_create("CompletionThread", &cpumask);
  printf("Create index and completion thread %s %lu\n",
         spdk_thread_get_name(mCompletionThread),
         spdk_thread_get_id(mCompletionThread));
  int rc = spdk_env_thread_launch_pinned(Configuration::GetCompletionThreadCoreId(), completionWorker, this);
  if (rc < 0) {
    printf("Failed to launch completion thread, error: %s\n", spdk_strerror(rc));
  }
}

void RAIDController::initDispatchThread()
{
  struct spdk_cpuset cpumask;
  spdk_cpuset_zero(&cpumask);
  spdk_cpuset_set_cpu(&cpumask, Configuration::GetDispatchThreadCoreId(), true);
  mDispatchThread = spdk_thread_create("DispatchThread", &cpumask);
  printf("Create dispatch thread %s %lu\n",
         spdk_thread_get_name(mDispatchThread),
         spdk_thread_get_id(mDispatchThread));
  int rc = spdk_env_thread_launch_pinned(Configuration::GetDispatchThreadCoreId(), dispatchWorker, this);
  if (rc < 0) {
    printf("Failed to launch dispatch thread error: %s %s\n", strerror(rc), spdk_strerror(rc));
  }
}

void RAIDController::initIoThread()
{
  struct spdk_cpuset cpumask;
  for (uint32_t threadId = 0; threadId < Configuration::GetNumIoThreads(); ++threadId) {
    spdk_cpuset_zero(&cpumask);
    spdk_cpuset_set_cpu(&cpumask, Configuration::GetIoThreadCoreId(threadId), true);
    mIoThread[threadId].thread = spdk_thread_create("IoThread", &cpumask);
    assert(mIoThread[threadId].thread != nullptr);
    mIoThread[threadId].controller = this;
    int rc = spdk_env_thread_launch_pinned(Configuration::GetIoThreadCoreId(threadId), ioWorker, &mIoThread[threadId]);
    printf("ZNS_RAID io thread %s %lu\n", spdk_thread_get_name(mIoThread[threadId].thread), spdk_thread_get_id(mIoThread[threadId].thread));
    if (rc < 0) {
      printf("Failed to launch IO thread error: %s %s\n", strerror(rc), spdk_strerror(rc));
    }
  }
}

void RAIDController::rebuild(uint32_t failedDriveId)
{
  struct timeval s, e;
  gettimeofday(&s, NULL);

  // Valid (full and open) zones and their headers
  std::map<uint64_t, uint8_t*> zonesAndHeaders[mN];
  // Segment ID to (wp, SegmentMetadata)
  std::map<uint32_t, std::vector<std::pair<uint64_t, uint8_t*>>> potentialSegments;
  for (uint32_t i = 0; i < mN; ++i) {
    if (i == failedDriveId) {
      continue;
    }

    mDevices[i]->ReadZoneHeaders(zonesAndHeaders[i]);
    for (auto zoneAndHeader : zonesAndHeaders[i]) {
      uint64_t wp = zoneAndHeader.first;
      SegmentMetadata *segMeta = reinterpret_cast<SegmentMetadata*>(zoneAndHeader.second);
      if (potentialSegments.find(segMeta->segmentId) == potentialSegments.end()) {
        potentialSegments[segMeta->segmentId].resize(mN);
      }
      potentialSegments[segMeta->segmentId][i] = std::pair(wp, zoneAndHeader.second);
    }
  }

  // Filter out invalid segments
  for (auto it = potentialSegments.begin();
       it != potentialSegments.end(); ) {
    auto &zones = it->second;
    bool isValid = true;

    SegmentMetadata *segMeta = nullptr;
    for (uint32_t i = 0; i < mN; ++i) {
      if (i == failedDriveId) {
        continue;
      }

      auto zone = zones[i];
      if (zone.second == nullptr) {
        isValid = false;
        break;
      }

      if (segMeta == nullptr) {
        segMeta = reinterpret_cast<SegmentMetadata*>(zone.second);
      }

      if (memcmp(segMeta, zone.second, sizeof(SegmentMetadata)) != 0) {
        isValid = false;
        break;
      }
    }

    if (!isValid) {
      it = potentialSegments.erase(it);
    } else {
      ++it;
    }
  }

  uint32_t numZones = mN;
  uint32_t zoneSize = 2 * 1024 * 1024 / 4;
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t metadataSize = Configuration::GetMetadataSize();
  // repair; allocate buffer in advance (read the whole zone from other drives)
  uint8_t *buffers[numZones];
  uint8_t *metaBufs[numZones];
  for (uint32_t i = 0; i < numZones; ++i) {
    buffers[i] = (uint8_t*)spdk_zmalloc(zoneSize * blockSize, 4096, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    metaBufs[i] = (uint8_t*)spdk_zmalloc(
      zoneSize * metadataSize, 4096, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    if (buffers[i] == nullptr || metaBufs[i] == nullptr) {
      debug_e("Failed to allocate memory. Increase the huge pages\n");
      assert(0);
    }
  }

  bool alive[numZones];
  for (uint32_t zoneId = 0; zoneId < numZones; ++zoneId) {
    if (zoneId == failedDriveId) {
      alive[zoneId] = false;
    } else {
      alive[zoneId] = true;
    }
  }

  for (auto it = potentialSegments.begin(); it != potentialSegments.end(); ++it) {
    // read metadata; the metadata is the same among all zones in the segment
    SegmentMetadata* segMeta = nullptr;
    
    auto &zones = it->second;
    for (uint32_t i = 0; i < mN; ++i) {
      if (i == failedDriveId) {
        continue;
      }
      auto zone = zones[i];
      if (segMeta == nullptr) {
        segMeta = reinterpret_cast<SegmentMetadata*>(zone.second);
        break;
      }
    }

    // prepare variables
    uint32_t groupSize = segMeta->stripeGroupSize;
    uint32_t numDataZones = segMeta->k; 
    uint32_t unitSize = segMeta->stripeUnitSize;
    uint32_t numStripeGroups = mDataRegionSize / (groupSize * unitSize /
      blockSize);
    // in-group offset to offset
    std::vector< std::vector<uint32_t> > stripeIdx2Offsets;
    stripeIdx2Offsets.resize(groupSize);
    for (uint32_t i = 0; i < groupSize; ++i) {
      stripeIdx2Offsets[i].resize(numZones);
    }

    // read the zones from other devices
    uint32_t offsets[numZones];
    for (uint32_t i = 0; i < numZones; ++i) {
      offsets[i] = 0;
    }

    while (true) {
      uint32_t counter = 0;
      for (uint32_t i = 0; i < numZones; ++i) {
        if (i == failedDriveId) {
          continue;
        }
        auto zone = zones[i];
        uint64_t wp = zone.first;
        uint32_t wpInZone = wp % zoneSize;
        uint64_t zslba = wp - wpInZone;
        uint32_t numBlocks2Fetch = std::min(wpInZone - offsets[i], 256u);

        if (numBlocks2Fetch > 0) {
          // update compact stripe table
          int error = 0;
          if ((error = spdk_nvme_ns_cmd_read_with_md(mDevices[i]->GetNamespace(),
                  mDevices[i]->GetIoQueue(0),
                  buffers[i] + offsets[i] * blockSize, metaBufs[i] + offsets[i] * metadataSize,
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
          if (i == failedDriveId) continue;
          spdk_nvme_qpair_process_completions(mDevices[i]->GetIoQueue(0), 0);
        }
      }
    }

    bool concluded = true;
    uint64_t validEnd = 0;
    // rebuild in memory
    if (Configuration::GetSystemMode() == ZAPRAID) {
      uint32_t stripeGroupId = 0;
      for (uint32_t stripeGroupIdx = 0; stripeGroupIdx < numStripeGroups; ++stripeGroupIdx) {
        uint64_t stripeGroupStart = mHeaderRegionSize + stripeGroupIdx * groupSize * unitSize / blockSize;
        uint64_t stripeGroupEnd = stripeGroupStart + groupSize * unitSize / blockSize;

        // Whether the current group is concluded
        for (uint32_t zoneIdx = 0; zoneIdx < numZones; ++zoneIdx) {
          if (zoneIdx == failedDriveId) {
            continue;
          }
          auto zone = zones[zoneIdx];
          uint64_t wp = zone.first;
          if (stripeGroupEnd > wp % zoneSize) {
            // the stripe group is not complete
            concluded = false;
            break;
          }
        }

        for (uint32_t i = 0; i < groupSize; ++i) {
          for (uint32_t j = 0; j < numZones; ++j) {
            stripeIdx2Offsets[i][j] = ~0;
          }
        }

        if (concluded) {
          for (uint32_t i = 0; i < numZones; ++i) {
            for (uint32_t j = 0; j < groupSize * unitSize / blockSize; ++j) {
              uint16_t stripeIdx = 0;
              if (i != failedDriveId) {
                stripeIdx = ((BlockMetadata*)(
                        metaBufs[i] + (stripeGroupStart + j) * metadataSize
                    ))->fields.replicated.stripeId;
              } else {
                stripeIdx = j / (unitSize / blockSize);
              }
              // the i-th block of stripeId-th stripe is the j-th block in the group.

              if (stripeIdx2Offsets[stripeIdx][i] == ~0) {
                assert(j % (unitSize / blockSize) == 0);
                stripeIdx2Offsets[stripeIdx][i] = j;
              } else {
                assert(stripeIdx2Offsets[stripeIdx][i] == 
                  round_down(j, unitSize / blockSize));
              }
            }
          }

          // Repair all the stripes
          for (uint32_t stripeIdx = 0; stripeIdx < groupSize; ++stripeIdx) {
            uint8_t *stripe[numZones];
            // decode data
            uint32_t stripeOffset = stripeIdx * unitSize / blockSize;
            for (uint32_t zoneIdx = 0; zoneIdx < numZones; ++zoneIdx) {
              uint32_t offsetInBlocks = stripeGroupStart + stripeIdx2Offsets[stripeIdx][zoneIdx];
              stripe[zoneIdx] = buffers[zoneIdx] + offsetInBlocks * blockSize;
            }
            uint32_t globalStripeId =
              Configuration::GetStripeIdFromOffset(unitSize, stripeGroupStart +
              stripeOffset);
            DecodeStripe(stripeGroupStart + stripeOffset, globalStripeId,
                stripe, alive, numZones, numDataZones,
                failedDriveId, unitSize);

            // decode metadata
            for (uint32_t zoneIdx = 0; zoneIdx < numZones; ++zoneIdx) {
              uint32_t offsetInBlocks = stripeGroupStart + stripeIdx2Offsets[stripeIdx][zoneIdx];
              stripe[zoneIdx] = reinterpret_cast<uint8_t*>(&(((BlockMetadata*)(metaBufs[zoneIdx] +
                        offsetInBlocks * metadataSize))->fields.coded));
            }
            DecodeStripe(stripeGroupStart + stripeOffset, globalStripeId, 
                stripe, alive, numZones, numDataZones,
                failedDriveId, unitSize / blockSize * metadataSize);
            ((BlockMetadata*)stripe[0])->fields.replicated.stripeId = stripeIdx;
          }
        } else {
          printf("NOT concluded.\n");
          for (uint32_t i = 0; i < groupSize; ++i) {
            for (uint32_t j = 0; j < numZones; ++j) {
              stripeIdx2Offsets[i][j] = ~0;
            }
          }

          for (uint32_t zoneIdx = 0; zoneIdx < numZones; ++zoneIdx) {
            if (zoneIdx == failedDriveId) {
              continue;
            }
            auto zone = zones[zoneIdx];
            uint64_t wp = zone.first;
            for (uint32_t blockIdx = stripeGroupStart;
                blockIdx < wp % zoneSize; ++blockIdx) {
              BlockMetadata *blockMetadata = (BlockMetadata*)(metaBufs[zoneIdx] + blockIdx * metadataSize);
              uint32_t stripeIdx = blockMetadata->fields.replicated.stripeId;
              if (stripeIdx2Offsets[stripeIdx][zoneIdx] == ~0u) {
                stripeIdx2Offsets[stripeIdx][zoneIdx] = blockIdx - stripeGroupStart;
              } else {
                assert(stripeIdx2Offsets[stripeIdx][zoneIdx] == 
                  round_down(blockIdx - stripeGroupStart, unitSize / blockSize));

              }
            }
          }

          uint32_t nextValidBlock = 0;
          // Repair all the stripes
          for (uint32_t stripeIdx = 0; stripeIdx < groupSize; ++stripeIdx) {
            uint8_t *stripe[numZones];
            bool validStripe = true;
            uint32_t stripeOffset = stripeIdx * unitSize / blockSize;
            for (uint32_t zoneIdx = 0; zoneIdx < numZones; ++zoneIdx) {
              if (zoneIdx == failedDriveId) {
                stripeIdx2Offsets[stripeIdx][zoneIdx] = nextValidBlock;
              }
              if (stripeIdx2Offsets[stripeIdx][zoneIdx] == ~0) {
                validStripe = false;
                break;
              }
              uint32_t offsetInBlocks = stripeGroupStart + stripeIdx2Offsets[stripeIdx][zoneIdx];
              stripe[zoneIdx] = buffers[zoneIdx] + offsetInBlocks * blockSize;
            }
            if (!validStripe) {
              continue;
            }
            uint32_t globalStripeId =
              Configuration::GetStripeIdFromOffset(unitSize, stripeGroupStart +
              stripeOffset);
            DecodeStripe(stripeGroupStart + stripeOffset, globalStripeId, 
                stripe, alive, numZones, numDataZones,
                failedDriveId, unitSize);

            // decode metadata
            for (uint32_t zoneIdx = 0; zoneIdx < numZones; ++zoneIdx) {
              uint32_t offsetInBlocks = stripeGroupStart + stripeIdx2Offsets[stripeIdx][zoneIdx];
              // stripe[zoneIdx] = (metaBufs[zoneIdx] + offsetInBlocks * metadataSize);
              stripe[zoneIdx] = reinterpret_cast<uint8_t*>(&(((BlockMetadata*)(metaBufs[zoneIdx] +
                        offsetInBlocks * metadataSize))->fields.coded));
            }
            DecodeStripe(stripeGroupStart + stripeOffset, globalStripeId,
                stripe, alive, numZones, numDataZones,
                failedDriveId, metadataSize * unitSize / blockSize);
            ((BlockMetadata*)stripe[0])->fields.replicated.stripeId = stripeIdx;
            nextValidBlock += unitSize / blockSize;
          }
          validEnd = stripeGroupStart + nextValidBlock;
        }

        if (!concluded) {
          // The last stripe group
          break;
        }
      }
    } else if (Configuration::GetSystemMode() == ZONEWRITE_ONLY) {
      for (uint32_t stripeOffset = 0; stripeOffset < mDataRegionSize; 
          stripeOffset += unitSize / blockSize) {
        uint32_t stripeIdx = stripeOffset / (unitSize / blockSize);
        uint8_t *dataStripe[numZones];
        uint8_t *metaStripe[numZones];
        for (uint32_t zoneIdx = 0; zoneIdx < numZones; ++zoneIdx) {
          auto zone = zones[zoneIdx];
          if (zoneIdx != failedDriveId && stripeOffset + mHeaderRegionSize >= zone.first % zoneSize) {
            printf("%u %lu %u\n", stripeIdx, zone.first, zoneSize);
            concluded = false;
            validEnd = mHeaderRegionSize + stripeOffset;
          }
          dataStripe[zoneIdx] = buffers[zoneIdx] + (mHeaderRegionSize + stripeOffset) * blockSize;
          metaStripe[zoneIdx] =
            reinterpret_cast<uint8_t*>(&(((BlockMetadata*)(metaBufs[zoneIdx] +
                      (mHeaderRegionSize + stripeOffset) *
                      metadataSize))->fields.coded));
        }
        if (!concluded) {
          printf("NOT concluded.\n");
          break;
        }

        uint32_t globalStripeId =
          Configuration::GetStripeIdFromOffset(unitSize, mHeaderRegionSize +
          stripeOffset);
        DecodeStripe(mHeaderRegionSize + stripeOffset, globalStripeId, 
            dataStripe, alive, numZones, numDataZones,
            failedDriveId, unitSize);
        DecodeStripe(mHeaderRegionSize + stripeOffset, globalStripeId, 
            metaStripe, alive, numZones, numDataZones,
            failedDriveId, metadataSize * unitSize / blockSize);
      }
    }

    if (concluded) {
    // construct the footer
      for (uint32_t offsetInFooterRegion = 0; offsetInFooterRegion < mFooterRegionSize; ++offsetInFooterRegion) {
        uint32_t begin = offsetInFooterRegion * (blockSize / 20);
        uint32_t end = std::min(mDataRegionSize, begin + blockSize / 20);

        for (uint32_t offsetInDataRegion = begin; offsetInDataRegion < end; ++offsetInDataRegion) {
          uint8_t *footer = buffers[0] + (mHeaderRegionSize + mDataRegionSize + offsetInFooterRegion) * blockSize + (offsetInDataRegion - begin) * 20;
          BlockMetadata *blockMetadata = (BlockMetadata*)(metaBufs[0] + (mHeaderRegionSize + offsetInDataRegion) * metadataSize);
          uint32_t stripeId = blockMetadata->fields.replicated.stripeId;
          uint32_t stripeOffset = (Configuration::GetSystemMode() == ZONEWRITE_ONLY) ?
              offsetInDataRegion : (round_down(offsetInDataRegion, groupSize *
                    unitSize / blockSize) + stripeId * unitSize / blockSize);
          uint32_t segStripeId = Configuration::GetStripeIdFromOffset(unitSize, stripeOffset + mHeaderRegionSize);
          if (Configuration::CalculateDiskId(segStripeId, numZones - 1,
                Configuration::GetRaidLevel(), numZones) == 0) {
            // set invalid LBA in the footer for praity blocks, or we just need to decide when reboot - it is only an implementation considerataion
            *(uint64_t*)(footer + 0) = ~0ull;
            *(uint64_t*)(footer + 8) = 0;
          } else {
            *(uint64_t*)(footer + 0) = blockMetadata->fields.coded.lba;
            *(uint64_t*)(footer + 8) = blockMetadata->fields.coded.timestamp;
          }
          *(uint32_t*)(footer +16) = blockMetadata->fields.replicated.stripeId;
        }
      }
      validEnd = mHeaderRegionSize + mDataRegionSize + mFooterRegionSize;
    }

    uint64_t slba = ((SegmentMetadata*)zones[1].second)->zones[0];
    printf("Write recovered zone to %lu\n", slba);
    // TODO why zero?
    memcpy(buffers[0], (SegmentMetadata*)zones[1].second, sizeof(SegmentMetadata));
    // write the zone to the failed drive
    for (uint64_t offset = 0; offset < validEnd; offset += 32) {
      int error = 0;
      bool done = false;
      uint32_t count = std::min(32u, (uint32_t)(validEnd - offset));
      if ((error = spdk_nvme_ns_cmd_write_with_md(
              mDevices[0]->GetNamespace(),
              mDevices[0]->GetIoQueue(0),
              buffers[0] + offset * blockSize, metaBufs[0] + offset * metadataSize,
              slba + offset, count, complete, &done, 0, 0, 0)) < 0) {
        printf("Error in writing %d %s.\n", error, strerror(error));
      }
      while (!done) {
        spdk_nvme_qpair_process_completions(mDevices[0]->GetIoQueue(0), 0);
      }
    }

    if (concluded) {
      bool done = false;
      if (spdk_nvme_zns_finish_zone(
            mDevices[0]->GetNamespace(),
            mDevices[0]->GetIoQueue(0),
            slba, 0, complete, &done) != 0) {
        fprintf(stderr, "Seal error in recovering footer region.\n");
      }
      while (!done) {
        spdk_nvme_qpair_process_completions(mDevices[0]->GetIoQueue(0), 0);
      }
    }
  }

  gettimeofday(&e, NULL);
  double elapsed = e.tv_sec - s.tv_sec + e.tv_usec / 1000000. - s.tv_usec / 1000000.;
  printf("Rebuild time: %.6f\n", elapsed);
}

void RAIDController::restart()
{
  uint64_t zoneCapacity = mDevices[0]->GetZoneCapacity();
  std::pair<uint64_t, PhysicalAddr> *indexMap =
    new std::pair<uint64_t, PhysicalAddr>[Configuration::GetStorageSpaceInBytes() / Configuration::GetBlockSize()];
  std::pair<uint64_t, PhysicalAddr> defaultAddr;
  defaultAddr.first = 0;
  defaultAddr.second.segment = nullptr;
  std::fill(indexMap, indexMap + Configuration::GetStorageSpaceInBytes() / Configuration::GetBlockSize(), defaultAddr);

  struct timeval s, e;
  gettimeofday(&s, NULL);
  // Mount from an existing new array
  // Reconstruct existing segments
  uint32_t zoneSize = 2 * 1024 * 1024 / 4;
  // Valid (full and open) zones and their headers
  std::map<uint64_t, uint8_t*> zonesAndHeaders[mN];
  // Segment ID to (wp, SegmentMetadata)
  std::map<uint32_t, std::vector<std::pair<uint64_t, uint8_t*>>> potentialSegments;

  debug_error("Reconstructing segments, rss %lu, storage space %lu\n", getRss(), Configuration::GetStorageSpaceInBytes());

  for (uint32_t i = 0; i < mN; ++i) {
    mDevices[i]->ReadZoneHeaders(zonesAndHeaders[i]);
    for (auto zoneAndHeader : zonesAndHeaders[i]) {
      uint64_t wp = zoneAndHeader.first;
      SegmentMetadata *segMeta = reinterpret_cast<SegmentMetadata*>(zoneAndHeader.second);
      if (potentialSegments.find(segMeta->segmentId) == potentialSegments.end()) {
        potentialSegments[segMeta->segmentId].resize(mN);
      }
      potentialSegments[segMeta->segmentId][i] = std::pair(wp, zoneAndHeader.second);
    }
  }

  // Filter out invalid segments
  for (auto it = potentialSegments.begin();
       it != potentialSegments.end(); ) {
    auto &zones = it->second;
    bool isValid = true;

    SegmentMetadata *segMeta = nullptr;
    for (uint32_t i = 0; i < mN; ++i) {
      auto zone = zones[i];
      if (zone.second == nullptr) {
        isValid = false;
        debug_error("header empty %d\n", i);
        break;
      }

      if (segMeta == nullptr) {
        segMeta = reinterpret_cast<SegmentMetadata*>(zone.second);
      }

      if (memcmp(segMeta, zone.second, sizeof(SegmentMetadata)) != 0) {
        isValid = false;
        debug_error("cast error %d\n", i);
        break;
      }
    }

    if (!isValid) {
      for (uint32_t i = 0; i < mN; ++i) {
        auto zone = zones[i];
        uint64_t wp = zone.first;
        SegmentMetadata *segMeta = reinterpret_cast<SegmentMetadata*>(zone.second);
        bool done = false;
        if (spdk_nvme_zns_reset_zone(mDevices[i]->GetNamespace(),
            mDevices[i]->GetIoQueue(0),
            segMeta->zones[i],
            0, complete, &done) != 0) {
          printf("Reset error during recovery\n");
        }
        while (!done) {
          spdk_nvme_qpair_process_completions(mDevices[i]->GetIoQueue(0), 0);
        }
        spdk_free(segMeta); // free the allocated metadata memory
      }
      it = potentialSegments.erase(it);
    } else {
      ++it;
    }
  }
  int cnt = 0;

  // reconstruct all open, sealed segments (Segment Table)
  for (auto it = potentialSegments.begin();
       it != potentialSegments.end(); ++it) {
    uint32_t segmentId = it->first;
    auto &zones = it->second;
    RequestContextPool *rqPool = mRequestContextPoolForSegments;
    ReadContextPool *rdPool = mReadContextPool;
    StripeWriteContextPool *wPool = nullptr;

    mNextAssignedSegmentId = std::max(segmentId + 1, mNextAssignedSegmentId);

    // Check the segment is sealed or not
    bool sealed = true;
    uint64_t segWp = zoneSize;
    SegmentMetadata *segMeta = nullptr;
    for (auto zone : zones) {
      segWp = std::min(segWp, zone.first % zoneSize);
      segMeta = reinterpret_cast<SegmentMetadata*>(zone.second);
    }
    Segment *seg = nullptr;
    if (segWp == zoneCapacity) {
      // sealed
      std::scoped_lock<std::shared_mutex> lock(mSegmentTableMutex);
      seg = new Segment(this, rqPool, rdPool, wPool, segMeta);
      for (uint32_t i = 0; i < segMeta->numZones; ++i) {
        Zone *zone = mDevices[i]->OpenZoneBySlba(segMeta->zones[i]);
        seg->AddZone(zone);
        uint32_t zoneId = zone->GetSlba() / zone->GetSize();
        mZoneToSegmentMap[i * mTotalNumSegments + zoneId] = seg;
        mSegIdToZoneIdMap[seg->GetSegmentId()].push_back(zoneId);
      }
      seg->SetSegmentStatus(SEGMENT_SEALED);
      mSealedSegments.insert(seg);
    } else {
      // We assume at most one open segment is created
      // TODO
      wPool = mStripeWriteContextPools[0];
      // open
      seg = new Segment(this, rqPool, rdPool, wPool, segMeta);
      std::scoped_lock<std::shared_mutex> lock(mSegmentTableMutex);
      for (uint32_t i = 0; i < segMeta->numZones; ++i) {
        Zone *zone = mDevices[i]->OpenZoneBySlba(segMeta->zones[i]);
        seg->AddZone(zone);
        uint32_t zoneId = zone->GetSlba() / zone->GetSize();
        mZoneToSegmentMap[i * mTotalNumSegments + zoneId] = seg;
        mSegIdToZoneIdMap[seg->GetSegmentId()].push_back(zoneId);
      }
      // TODO check this
      mOpenSegments[0] = seg;
//      mOpenSegments.push_back(seg);
      seg->SetSegmentStatus(SEGMENT_NORMAL);
    }
    seg->SetZonesAndWpForRecovery(zones);
  }

  // For open segment: seal it if needed (position >= data region size)
  debug_error("Load all blocks of open segments. sealed %d\n",
      (int)mSealedSegments.size());
  printf("Load all blocks of open segments.\n");
  for (uint32_t i = 0; i < mNumOpenSegments; ++i) {
    debug_error("Load all blocks of open segments %d %p\n", i, mOpenSegments[i]);
    if (mOpenSegments[i] != nullptr) {
      mOpenSegments[i]->RecoverLoadAllBlocks();
      if (mOpenSegments[i]->RecoverFooterRegionIfNeeded()) {
        mSealedSegments.insert(mOpenSegments[i]);
        mOpenSegments[i] = nullptr;
      }
    }
  }

  debug_error("Recover stripe consistency. selaed %d\n",
      (int)mSealedSegments.size());
  printf("Recover stripe consistency.\n");
  // For open segment: recover stripe consistency
  for (uint32_t i = 0; i < mNumOpenSegments; ++i) {
    if (mOpenSegments[i] != nullptr) {
      if (mOpenSegments[i]->RecoverNeedRewrite()) {
        printf("Need rewrite.\n");
        SegmentMetadata segMeta = mOpenSegments[i]->DumpSegmentMeta();
        Segment *newSeg = new Segment(this, 
            mRequestContextPoolForSegments,
            mReadContextPool,
            mStripeWriteContextPools[i], &segMeta);
        for (uint32_t j = 0; j < mOpenSegments[i]->GetZones().size(); ++j) {
          std::scoped_lock<std::shared_mutex> w_lock(mSegmentTableMutex);
          Zone *zone = mDevices[j]->OpenZone();
          newSeg->AddZone(zone);
          uint32_t zoneId = zone->GetSlba() / zone->GetSize();
          mZoneToSegmentMap[i * mTotalNumSegments + zoneId] = newSeg;
          mSegIdToZoneIdMap[newSeg->GetSegmentId()].push_back(zoneId);
        }
        newSeg->RecoverFromOldSegment(mOpenSegments[i]);
        // reset old segment
        mOpenSegments[i]->ResetInRecovery();
        mOpenSegments[i] = newSeg;
      } else {
        printf("Recover in-flight stripes.\n");
        mOpenSegments[i]->RecoverState();
      }
    } else {
      printf("NO open segment.\n");
    }
  }

  printf("Recover index from sealed segments.\n");
  // Start recover L2P table and the compact stripe table
  uint8_t *buffer = (uint8_t*)spdk_zmalloc(
      std::max(mFooterRegionSize, (uint32_t)round_up(mDataRegionSize, 20)
//        mDataRegionSize / Configuration::GetStripeGroupSize()
        )
      * Configuration::GetBlockSize(), 4096,
      NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
  // all segments share the same index map
  for (Segment *segment : mSealedSegments) {
    segment->RecoverIndexFromSealedSegment(buffer, indexMap);
  }
  spdk_free(buffer);
  uint32_t blockSize = Configuration::GetBlockSize();

  printf("Recover index from open segments.\n");
  for (Segment *segment : mOpenSegments) {
    if (segment) {
      segment->RecoverIndexFromOpenSegment(indexMap);
    }
  }

  for (uint32_t i = 0;
      i < Configuration::GetStorageSpaceInBytes() / blockSize;
      ++i) {
    if (indexMap[i].second.segment == nullptr) {
      continue;
    }
    uint32_t pbaInBlk;
    std::shared_lock<std::shared_mutex> r_lock(mSegmentTableMutex);
    PhysicalAddr& pba = indexMap[i].second;
    uint32_t segId = pba.segment->GetSegmentId();
    assert(mSegIdToZoneIdMap.count(segId) && mSegIdToZoneIdMap.at(segId).size() == mN);

    pbaInBlk = pba.zoneId * mTotalNumSegments * mZoneSize +
      mSegIdToZoneIdMap[segId][pba.zoneId] * mZoneSize + pba.offset;
    IndexMapStatus s;
    uint32_t oldMapPba;
    s = mAddressMap->UpdatePba(i, pbaInBlk, oldMapPba);
    if (s == kHit) {
//      mAddressMapMemory[i] = pbaInBlk;
      if (oldMapPba != ~0u) {
        printf("old pba %u\n", oldMapPba);
        assert(0);
      }
    }
    if (s != kHit) {
      printf("Index map full.\n");
      assert(0);
    }

    
//    mAddressMap[i] = indexMap[i].second;
    pba.segment->FinishBlock(pba.zoneId, pba.offset, (uint64_t)i *
        blockSize);
//    mAddressMap[i].segment->FinishBlock(
//        mAddressMap[i].zoneId,
//        mAddressMap[i].offset,
//        i * Configuration::GetBlockSize() * 1ull);
  }

  for (Segment *segment : mSealedSegments) {
    segment->FinishRecovery();
  }

  for (Segment *segment : mOpenSegments) {
    if (segment) {
      segment->FinishRecovery();
    }
  }
  gettimeofday(&e, NULL);
  double elapsed = e.tv_sec - s.tv_sec + e.tv_usec / 1000000. - s.tv_usec / 1000000.;
  printf("Restart time: %.6f\n", elapsed);
}

void RAIDController::Init(bool need_env)
{
  int ret = 0;
  struct timeval tv1;
  gettimeofday(&tv1, NULL);
  StatsRecorder::getInstance()->openStatistics(tv1);
  if (need_env) {
    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.core_mask = "0x1fc";
    if (spdk_env_init(&opts) < 0) {
      fprintf(stderr, "Unable to initialize SPDK env.\n");
      exit(-1);
    }

    ret = spdk_thread_lib_init(nullptr, 0);
    if (ret < 0) {
      fprintf(stderr, "Unable to initialize SPDK thread lib.\n");
      exit(-1);
    }
  }

  ret = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL); 
  if (ret < 0) {
    fprintf(stderr, "Unable to probe devices\n");
    exit(-1);
  }

  // init devices
  mDevices = g_devices;
  std::sort(mDevices.begin(), mDevices.end(), [](const Device *o1, const Device *o2) -> bool {
      // there will be no tie between two PCIe address, so no issue without equality test
        return strcmp(o1->GetDeviceTransportAddress(), o2->GetDeviceTransportAddress()) < 0;
      });

  InitErasureCoding();
  if (mDevices.size() == 0) {
    debug_e("No NVMe device detected!\n");
    exit(1);
  }
  // the value of n in erasure coding
  mN = mDevices.size();

  // Adjust the capacity for user data = total capacity - footer size
  // The L2P table information at the end of the segment
  // Each block needs (LBA + timestamp + stripe ID, 20 bytes) for L2P table recovery; we round the number to block size
  mZoneSize = mDevices[0]->GetZoneSize();
  uint64_t zoneCapacity = mDevices[0]->GetZoneCapacity();
  uint32_t blockSize = Configuration::GetBlockSize();
  uint64_t storageSpace = Configuration::GetStorageSpaceInBytes();
  mMappingBlockUnitSize = blockSize * blockSize / 4; 
  uint32_t maxFooterSize = round_up(zoneCapacity, (blockSize / 20)) / (blockSize / 20);
  mHeaderRegionSize = Configuration::GetMaxStripeUnitSize() / blockSize;
  //  1;
  mDataRegionSize = round_down(zoneCapacity - mHeaderRegionSize - maxFooterSize,
                               Configuration::GetMaxStripeGroupSizeInBlocks());
  mFooterRegionSize = round_up(mDataRegionSize, (blockSize / 20)) / (blockSize / 20);
  printf("HeaderRegion: %u, DataRegion: %u, FooterRegion: %u\n",
         mHeaderRegionSize, mDataRegionSize, mFooterRegionSize);

  uint32_t totalNumZones = round_up(storageSpace / blockSize,
                                    mDataRegionSize) / mDataRegionSize;
  uint32_t numDataBlocks = Configuration::GetStripeDataSize() / Configuration::GetStripeUnitSize();
  uint32_t numZonesNeededPerDevice = round_up(totalNumZones, numDataBlocks) / numDataBlocks;
  uint32_t numZonesReservedPerDevice = std::max(3u, (uint32_t)(numZonesNeededPerDevice * 0.25));

  for (uint32_t i = 0; i < mN; ++i) {
    mDevices[i]->SetDeviceId(i);
    mDevices[i]->InitZones(numZonesNeededPerDevice, numZonesReservedPerDevice);
  }

  mStorageSpaceThresholdForGcInSegments = numZonesReservedPerDevice / 2;
  mAvailableStorageSpaceInSegments = numZonesNeededPerDevice + numZonesReservedPerDevice;
  mTotalNumSegments = mAvailableStorageSpaceInSegments;
  printf("Total available segments: %u, reserved segments: %u\n", mAvailableStorageSpaceInSegments, mStorageSpaceThresholdForGcInSegments);
  mZoneToSegmentMap = new Segment*[mTotalNumSegments * mN];
  memset(mZoneToSegmentMap, 0, sizeof(Segment*) * mTotalNumSegments * mN);

  // Preallocate contexts for user requests
  // Sufficient to support multiple I/O queues of NVMe-oF target
  mRequestContextPoolForUserRequests = new RequestContextPool(2048);
  mRequestContextPoolForSegments = new RequestContextPool(4096);
  mRequestContextPoolForIndex = new RequestContextPool(128);

  mReadContextPool = new ReadContextPool(512, mRequestContextPoolForSegments);

  // Initialize address map
  mAddressMap = new IndexMap(storageSpace / blockSize, 
      Configuration::GetL2PTableSizeInBytes() / blockSize); 
//  mAddressMap = new IndexMap(storageSpace / blockSize, 0); 
//  mAddressMapMemory = new uint32_t[storageSpace / blockSize];
//  memset(mAddressMapMemory, 0xff, sizeof(uint32_t) * storageSpace / blockSize);

  // Create poll groups for the io threads and perform initialization
  for (uint32_t threadId = 0; threadId < Configuration::GetNumIoThreads(); ++threadId) {
    mIoThread[threadId].group = spdk_nvme_poll_group_create(NULL, NULL);
    mIoThread[threadId].controller = this;
  }
  for (uint32_t i = 0; i < mN; ++i) {
    struct spdk_nvme_qpair** ioQueues = mDevices[i]->GetIoQueues();
    for (uint32_t threadId = 0; threadId < Configuration::GetNumIoThreads(); ++threadId) {
      spdk_nvme_ctrlr_disconnect_io_qpair(ioQueues[threadId]);
      int rc = spdk_nvme_poll_group_add(mIoThread[threadId].group, ioQueues[threadId]);
      assert(rc == 0);
    }
    mDevices[i]->ConnectIoPairs();
  }

  // Preallocate segments
  mNumOpenSegments = Configuration::GetNumOpenSegments();
  mOpenGroupForLarge = new bool[mNumOpenSegments];
  for (int i = 0; i < mNumOpenSegments; ++i) {
    mOpenGroupForLarge[i] = Configuration::GetStripeUnitSize(i) >=
      Configuration::GetLargeRequestThreshold();
  }

  mStripeWriteContextPools = new StripeWriteContextPool *[mNumOpenSegments + 2];
  for (uint32_t i = 0; i < mNumOpenSegments + 2; ++i) {
    bool flag = i < mNumOpenSegments ? 
      (Configuration::GetStripeGroupSize(i) == 1) : 
      (Configuration::GetStripeGroupSize(mNumOpenSegments - 1) == 1);
//      mStripeWriteContextPools[i] = new StripeWriteContextPool(16, mRequestContextPoolForSegments);
    if (Configuration::GetSystemMode() == ZONEWRITE_ONLY || 
        Configuration::GetSystemMode() == RAIZN_SIMPLE || 
        (Configuration::GetSystemMode() == ZAPRAID && flag))
    {
//      mStripeWriteContextPools[i] = new StripeWriteContextPool(1, mRequestContextPoolForSegments);
//      mStripeWriteContextPools[i] = new StripeWriteContextPool(3, mRequestContextPoolForSegments);
      mStripeWriteContextPools[i] = new StripeWriteContextPool(3, mRequestContextPoolForSegments);
      printf("create segment for zone write (i=%d)\n", i);
      // a large value causes zone-writes to be slow
    } else {
      uint32_t index = i;
      if (index >= mNumOpenSegments) {
        index = mNumOpenSegments - 1;
      }
      printf("create segment for zone append (i=%d) flag %d unit size %d mode %d\n", i,
          flag, Configuration::GetStripeUnitSize(index), Configuration::GetSystemMode());
//      mStripeWriteContextPools[i] = new StripeWriteContextPool(64, mRequestContextPoolForSegments); // try fewer for femu devices
      mStripeWriteContextPools[i] = new StripeWriteContextPool(16, mRequestContextPoolForSegments); // for real devices, can have more contexts
    }
  }

  mOpenSegments.resize(mNumOpenSegments);

  debug_error("reboot mode %d\n", Configuration::GetRebootMode());
  if (Configuration::GetRebootMode() == 0) {
    for (uint32_t i = 0; i < mN; ++i) {
      debug_warn("Erase device %u\n", i);
      mDevices[i]->EraseWholeDevice();
    }
  } else if (Configuration::GetRebootMode() == 1) {
    debug_e("restart()");
    restart();
  } else { // needs rebuild; rebootMode = 2
    // Suppose drive 0 is broken
    mDevices[0]->EraseWholeDevice();
    rebuild(0); // suppose rebuilding drive 0
    restart();
  }

  if (Configuration::GetEventFrameworkEnabled()) {
    event_call(Configuration::GetDispatchThreadCoreId(),
               registerDispatchRoutine, this, nullptr);
    for (uint32_t threadId = 0;
         threadId < Configuration::GetNumIoThreads(); 
         ++threadId) {
      event_call(Configuration::GetIoThreadCoreId(threadId),
                 registerIoCompletionRoutine, &mIoThread[threadId], nullptr);
    }
  } else {
    initIoThread();
    initDispatchThread();
    initIndexThread();
    initCompletionThread();
    initEcThread();
  }

  // create initialized segments
  for (uint32_t i = 0; i < mNumOpenSegments; ++i) {
    createSegmentIfNeeded(&mOpenSegments[i], i);
  }

  // init Gc
  initGc();

  // debug
  mStartedWrites = 0;
  mCompletedWrites = 0;
  mStartedReads = 0;
  mCompletedReads = 0;
  mNumIndexWrites = 0;
  mNumIndexWritesHandled = 0;
  mNumIndexReads = 0;
  mNumIndexReadsHandled = 0;

  Configuration::PrintConfigurations();
  debug_error("init finished %p\n", this);
}

RAIDController::~RAIDController()
{
  int cnt = 0;
  while (true) {
    if (!ExistsGc()) {
      cnt++;
      if (cnt >= 200) break; // no GC work for 200ms
    } else {
      cnt = 0;
    }
    usleep(1000);
  }

  Dump();

  delete mAddressMap;
  if (!Configuration::GetEventFrameworkEnabled()) {
    for (uint32_t i = 0; i < Configuration::GetNumIoThreads(); ++i) {
      thread_send_msg(mIoThread[i].thread, quit, nullptr);
    }
    thread_send_msg(mDispatchThread, quit, nullptr);
    thread_send_msg(mEcThread, quit, nullptr);
    thread_send_msg(mIndexThread, quit, nullptr);
    thread_send_msg(mCompletionThread, quit, nullptr);
  }

  if (!Configuration::GetEventFrameworkEnabled()) {
    for (uint32_t i = 0; i < Configuration::GetNumIoThreads(); ++i) {
//      thread_send_msg(mIoThread[i].thread, quit, nullptr);
      spdk_thread_exit(mIoThread[i].thread);
    }
//    thread_send_msg(mDispatchThread, quit, nullptr);
//    thread_send_msg(mEcThread, quit, nullptr);
//    thread_send_msg(mIndexThread, quit, nullptr);
//    thread_send_msg(mCompletionThread, quit, nullptr);
    spdk_thread_exit(mDispatchThread);
    spdk_thread_exit(mEcThread);
    spdk_thread_exit(mIndexThread);
    spdk_thread_exit(mCompletionThread);
  }
}


void RAIDController::initGc()
{
  mGcTask.numBuffers = 32;
  mGcTask.dataBuffer = (uint8_t*)spdk_zmalloc(
                     mGcTask.numBuffers * Configuration::GetMaxStripeUnitSize(), 4096,
                     NULL, SPDK_ENV_SOCKET_ID_ANY,
                     SPDK_MALLOC_DMA);
  mGcTask.metaBuffer = (uint8_t*)spdk_zmalloc(
                     mGcTask.numBuffers *
                     (Configuration::GetMaxStripeUnitSize() /
                      Configuration::GetBlockSize()) *
                     Configuration::GetMetadataSize(),
                     4096, NULL, SPDK_ENV_SOCKET_ID_ANY,
                     SPDK_MALLOC_DMA);
  mGcTask.contextPool = new RequestContext[mGcTask.numBuffers];
  mGcTask.stage = IDLE;
}

// called by index thread 
uint32_t RAIDController::GcBatchUpdateIndex(
    const std::vector<uint64_t> &lbas,
    const std::vector<std::pair<PhysicalAddr, PhysicalAddr>> &pbas)
{

  uint32_t numSuccessUpdates = 0;
  assert(lbas.size() == pbas.size());
  uint32_t numInvalidated = 0;
  IndexMapStatus s;
  uint32_t blockSize = Configuration::GetBlockSize();
  int rollbackIdx = lbas.size();
  bool rollingBack = false;
  for (int i = 0; i < lbas.size(); ++i) {
    uint64_t lba = lbas[i];
    PhysicalAddr oldPba = pbas[i].first;
    PhysicalAddr newPba = pbas[i].second;
    PhysicalAddr thisPba; 

    // an mapping block
    if (lba % blockSize > 0) {
      if (rollingBack) {
        // do not update the mapping if gone into rollback mode
        continue;
      }

      assert((lba & (blockSize - 1)) == blockSize - 1);
      uint32_t oldPbaInBlk, newPbaInBlk; 
      uint32_t oldSegId = oldPba.segment->GetSegmentId();
      uint32_t newSegId = newPba.segment->GetSegmentId();
      {
        std::shared_lock<std::shared_mutex> r_lock(mSegmentTableMutex);
        assert(mSegIdToZoneIdMap.count(oldSegId));
        assert(mSegIdToZoneIdMap[oldSegId].size() == mN);
        assert(mSegIdToZoneIdMap.count(newSegId));
        assert(mSegIdToZoneIdMap[newSegId].size() == mN);
        oldPbaInBlk = oldPba.zoneId * mTotalNumSegments * mZoneSize +
          (uint64_t)mSegIdToZoneIdMap[oldSegId][oldPba.zoneId] * mZoneSize +
          oldPba.offset;
        newPbaInBlk = newPba.zoneId * mTotalNumSegments * mZoneSize +
          (uint64_t)mSegIdToZoneIdMap[newSegId][newPba.zoneId] * mZoneSize +
        newPba.offset;
      }
      IndexMapStatus t;
      t = mAddressMap->UpdateMappingBlockPba(lba, oldPbaInBlk, newPbaInBlk);
      if (t == kUpdatedSuccess) {
        // invalidate the old mapping block
        oldPba.segment->InvalidateBlock(oldPba.zoneId, oldPba.offset);
      } else if (t == kUpdatedFailedForInvalid) {
        // invalidate the new mapping block
        newPba.segment->InvalidateBlock(newPba.zoneId, newPba.offset);
      } else {
        debug_error("t = %d\n", t);
        assert(0);
      }
    } else {
      // normal data blocks
      uint32_t mappedPba;
      s = mAddressMap->GetPba(lba / blockSize, mappedPba);
      if (s == kHit) {
//        printf("get pba: lba %lu pba %u %u\n", lba, mappedPba, mAddressMapMemory[lba / blockSize]);
//        if (mAddressMapMemory[lba / blockSize] != mappedPba) {
//          debug_error("index error: lba %lu should be %u but %u\n", lba,
//              mAddressMapMemory[lba / blockSize], mappedPba);
//          assert(0);
//        }
      }

      if (s != kHit) {
        // the first not hit. Rollback, and prepare for reads 
        if (rollbackIdx >= lbas.size()) {
          rollbackIdx = i;
          rollingBack = true;
        }

        // prepare for write or reads
        if (s == kNeedEvict || s == kNeedRead) {
          auto ctx = PrepareIndexContext(s, lba);
          if (ctx != nullptr) {
            ExecuteIndex(ctx);
          } else {
            // too many evictions. Wait for a while
            break;
          }
        } else if (s == kFetching) {
          // waiting for fetching, check the next block
          continue;
        } else {
          debug_error("s = %d\n", s);
          assert(0);
        }
      }


      if (rollingBack) {
        // triggerred rolling back, do not update following mappings 
        continue;
      }

      // update the mappings
      if (mappedPba == ~0u) {
        debug_error("pba not found %lu\n", lba);
        assert(0);
      }
      // get segment, pba
      {
        std::shared_lock<std::shared_mutex> r_lock(mSegmentTableMutex);
        thisPba.segment = mZoneToSegmentMap[mappedPba / mZoneSize];
      }
      thisPba.zoneId = mappedPba / mZoneSize / mTotalNumSegments;
      thisPba.offset = mappedPba % mZoneSize;
      if (thisPba.segment == nullptr) {
        printf("[ERROR] Missing old lba %lu (pba in block %u gzone id %u)\n",
            lba, mappedPba, mappedPba / mZoneSize);
        assert(0);
      }
      // make sure that PBA does not change during the GC
      if (thisPba == oldPba) {
        numSuccessUpdates += 1;
        if (Configuration::DebugMode()) {
          printf("%s %d: update index in GC for lba %lu\n", __FILE__, __LINE__,
              lba); 
        }
        IndexMapStatus s = UpdateIndex(lba, newPba);
        if (s != kHit) {
          debug_e("Index map full.");
          // should not happen because the cache is hit before
          debug_error("How could this happen? (s = %d)\n", s);
          assert(0);
        }
      } else {
        // if the PBA changes during GC, the block is updated
        newPba.segment->InvalidateBlock(newPba.zoneId, newPba.offset);
      }
    }
  }

  // rollback if necessary
  for (int j = rollbackIdx; j < lbas.size(); ++j) {
    mGcTask.mappings.insert(std::make_pair(lbas[j], pbas[j]));
  }
  return numSuccessUpdates;
}

IndexMapStatus RAIDController::UpdateIndex(uint64_t lba, PhysicalAddr pba)
{
  // Invalidate the old block
  if (lba >= Configuration::GetStorageSpaceInBytes()) {
    printf("Error\n");
    assert(0);
  }
  uint32_t pbaInBlk = 0;
  uint32_t blockSize = Configuration::GetBlockSize();
  IndexMapStatus s;
  s = mAddressMap->GetPba(lba / blockSize, pbaInBlk);
  if (s == kHit) {
//    printf("get pba: lba %lu pba %u %u\n", lba, pbaInBlk, mAddressMapMemory[lba / blockSize]);
//    if (mAddressMapMemory[lba / blockSize] != pbaInBlk) {
//      debug_error("index error: lba %lu should be %u but %u\n", lba,
//          mAddressMapMemory[lba / blockSize], pbaInBlk);
//      Segment* seg = nullptr;
//      seg->IsFull();
//    }
  }
  if (s != kHit) {
    if (s == kNeedEvict || s == kNeedRead || s == kFetching) {
      return s;
    }
    debug_error("s = %d\n", s);
    assert(0);
  }
  PhysicalAddr oldPba;
  if (pbaInBlk == ~0u) {
    oldPba.segment = nullptr;
  } else {
    std::shared_lock<std::shared_mutex> r_lock(mSegmentTableMutex);
    oldPba.segment = mZoneToSegmentMap[pbaInBlk / mZoneSize];
    if (oldPba.segment == nullptr) {
      // has problem
      debug_error("old pba segment is null %lu pba %u zid %u\n", lba,
          pbaInBlk, pbaInBlk / mZoneSize);
//      debug_error("map memory: %u\n", mAddressMapMemory[lba / blockSize]);
      Segment* seg = nullptr;
      seg->IsFull();
    }
    oldPba.zoneId = pbaInBlk / mZoneSize / mTotalNumSegments;
    oldPba.offset = pbaInBlk % mZoneSize;
    if (pbaInBlk == 0) {
      debug_error("lba %lu\n", lba);
    }
    assert(oldPba.zoneId < mN);
    assert(oldPba.offset >= mHeaderRegionSize); 
    assert(oldPba.offset < mHeaderRegionSize + mDataRegionSize); 
  }

  if (oldPba.segment != nullptr && !(oldPba == pba)) {
    // only invalidate the data block if two addresses are different
    oldPba.segment->InvalidateBlock(oldPba.zoneId, oldPba.offset);
  }

  assert(pba.segment != nullptr);
  uint32_t segId = pba.segment->GetSegmentId();
  uint32_t newPbaInBlk, oldMapPba;
  {
    std::shared_lock<std::shared_mutex> r_lock(mSegmentTableMutex);
    assert(mSegIdToZoneIdMap.count(segId) && mSegIdToZoneIdMap.at(segId).size() == mN);
    newPbaInBlk = pba.zoneId * mTotalNumSegments * mZoneSize +
      (uint64_t)mSegIdToZoneIdMap[segId][pba.zoneId] * mZoneSize +
      pba.offset;
  }

//  if (oldPba == pba || newPbaInBlk == pbaInBlk) {
//    debug_error("cannot teleport to the original place: newPbaInBlk %u pbaInBlk %u\n", newPbaInBlk, pbaInBlk);
//    printf("update Pba lba %lu (%lu) from %u to %u (seg %u to %u)\n", 
//        lba / blockSize, lba, pbaInBlk, newPbaInBlk,
//        (oldPba.segment ? oldPba.segment->GetSegmentId() : -1), 
//        pba.segment->GetSegmentId());
//    Segment* seg = nullptr;
//    seg->IsFull();
//  } 

  // if a block becomes dirty, remember to invalidate the block
  s = mAddressMap->UpdatePba(lba / blockSize, newPbaInBlk, oldMapPba);
  if (s == kHit) {
//    mAddressMapMemory[lba / blockSize] = newPbaInBlk; 
    if (oldMapPba != ~0u) {
      // invalidate the old mapping block
      std::shared_lock<std::shared_mutex> r_lock(mSegmentTableMutex);
      PhysicalAddr oldPba;
      oldPba.segment = mZoneToSegmentMap[oldMapPba / mZoneSize];
      oldPba.zoneId = oldMapPba / mZoneSize / mTotalNumSegments;
      oldPba.offset = oldMapPba % mZoneSize;
      if (oldPba.segment != nullptr) {
        // invalidate mapping block
        oldPba.segment->InvalidateBlock(oldPba.zoneId, oldPba.offset);
      }
    }
  }
  if (s != kHit) {
    if (s == kNeedEvict || s == kNeedRead || s == kFetching) {
      return s;
    }
    debug_error("s = %d\n", s);
    assert(0);
  }
  return kHit;
}

void RAIDController::Write(
    uint64_t offset, uint32_t size, void* data,
    zns_raid_request_complete cb_fn, void *cb_args)
{
  mStartedWrites++;
  uint64_t blockSize = Configuration::GetBlockSize();
//  printf("write %lu offset %lu size %u\n", mStartedWrites.load(), offset,
//      size); 
  if (mStartedWrites % 1000000 == 0) {
    printf("[INFO] start write %lu %u cnt %lu (written %lu bytes)\n", offset,
        size, mStartedWrites.load(), 
        StatsRecorder::getInstance()->getTotalWriteBytes());
  }
  if (Configuration::GetEventFrameworkEnabled()) {
    Request *req = (Request*)calloc(1, sizeof(Request));
    req->controller = this;
    req->offset = offset;
    req->size = size;
    req->data = data;
    req->type = 'W';
    req->cb_fn = cb_fn;
    req->cb_args = cb_args;
    // will finally turn to Execute()
    event_call(Configuration::GetReceiverThreadCoreId(),
        executeRequest, req, nullptr);
  } else {
    Execute(offset, size, data, true, cb_fn, cb_args);
  }
}

void RAIDController::Read(
    uint64_t offset, uint32_t size, void* data,
    zns_raid_request_complete cb_fn, void *cb_args)
{
  mStartedReads++;
  if (mStartedReads % 1000000 == 0 || mStartedReads <= 2) {
    printf("start read %lu %u cnt %lu\n", offset, size, mStartedReads.load());
  }
  if (Configuration::GetEventFrameworkEnabled()) {
    Request *req = (Request*)calloc(1, sizeof(Request));
    req->controller = this;
    req->offset = offset;
    req->size = size;
    req->data = data;
    req->type = 'R';
    req->cb_fn = cb_fn;
    req->cb_args = cb_args;
    event_call(Configuration::GetReceiverThreadCoreId(),
        executeRequest, req, nullptr);
  } else {
    Execute(offset, size, data, false, cb_fn, cb_args);
  }
}

void RAIDController::CompleteWrite() {
  mCompletedWrites++;
}

void RAIDController::CompleteRead() {
  mCompletedReads++;
}

void RAIDController::RetrieveWRCounts(uint64_t& sw, uint64_t& sr, uint64_t& cw, uint64_t& cr) {
  sw = mStartedWrites;
  sr = mStartedReads;
  cw = mCompletedWrites;
  cr = mCompletedReads;
}

void RAIDController::RetrieveIndexWRCounts(uint64_t& sw, uint64_t& sr, uint64_t& cw, uint64_t& cr) {
  sw = mNumIndexWrites;
  sr = mNumIndexReads;
  cw = mNumIndexWritesHandled;
  cr = mNumIndexReadsHandled;
}

// dispatch thread
void RAIDController::ReclaimContexts()
{
  int numSuccessfulReclaims = 0;
  for (auto it = mInflightUserRequestContext.begin();
            it != mInflightUserRequestContext.end(); ) {
    if ((*it)->available) {
      (*it)->Clear();
      mRequestContextPoolForUserRequests->ReturnRequestContext(*it);
      it = mInflightUserRequestContext.erase(it);

      numSuccessfulReclaims++;
      if (numSuccessfulReclaims >= 128) {
        break;
      }
    } else {
      ++it;
    }
  }
}

void RAIDController::ReclaimIndexContexts() {
  int numSuccessfulReclaims = 0;
  for (auto it = mInflightIndexRequestContext.begin();
            it != mInflightIndexRequestContext.end(); ) {
    if ((*it)->available) {
      (*it)->Clear();
      mRequestContextPoolForIndex->ReturnRequestContext(*it);
      it = mInflightIndexRequestContext.erase(it);

      numSuccessfulReclaims++;
      if (numSuccessfulReclaims >= 128) {
        break;
      }
    } else {
      ++it;
    }
  }
}

void RAIDController::Flush()
{
  bool remainWrites;
  do {
    remainWrites = false;
    for (auto it = mInflightUserRequestContext.begin();
              it != mInflightUserRequestContext.end(); ) {
      if ((*it)->available) {
        (*it)->Clear();
        mRequestContextPoolForUserRequests->ReturnRequestContext(*it);
        it = mInflightUserRequestContext.erase(it);
      } else {
        if ((*it)->req_type == 'W') {
          remainWrites = true;
        }
        ++it;
      }
    }
  } while (remainWrites);
}

// dispatch thread
RequestContext* RAIDController::getContextForUserRequest()
{
  RequestContext *ctx = mRequestContextPoolForUserRequests->GetRequestContext(false);
  while (ctx == nullptr) {
    ReclaimContexts();
    ctx = mRequestContextPoolForUserRequests->GetRequestContext(false);
    if (ctx == nullptr) {
//      printf("NO AVAILABLE CONTEXT FOR USER.\n");
    }
  }

  mInflightUserRequestContext.insert(ctx);
  ctx->Clear();
  ctx->available = false;
  ctx->meta = nullptr;
  ctx->ctrl = this;
  return ctx;
}

RequestContext* RAIDController::getContextForIndex()
{
  RequestContext *ctx = mRequestContextPoolForIndex->GetRequestContext(false);
  while (ctx == nullptr) {
    ReclaimIndexContexts();
    ctx = mRequestContextPoolForIndex->GetRequestContext(false);
  }

  mInflightIndexRequestContext.insert(ctx);
  ctx->Clear();
  ctx->data = ctx->dataBuffer;
  ctx->available = false;
  ctx->meta = nullptr;
  ctx->ctrl = this;
  return ctx;
}

void RAIDController::Execute(
    uint64_t offset, uint32_t size, void *data, bool is_write,
    zns_raid_request_complete cb_fn, void *cb_args)
{
  RequestContext *ctx = getContextForUserRequest();
  ctx->type = USER;
  ctx->data = (uint8_t*)data;
  ctx->lba = offset;
  ctx->size = size;
  ctx->targetBytes = size;
  ctx->cb_fn = cb_fn;
  ctx->cb_args = cb_args;
  if (is_write) {
    debug_warn("ctx %p successBytes %u\n", ctx, ctx->successBytes);
    ctx->req_type = 'W';
    ctx->status = WRITE_REAPING;
    gettimeofday(&ctx->timeA, 0);
  } else {
    ctx->req_type = 'R';
    ctx->status = READ_PREPARE;
  }
  gettimeofday(&ctx->timeC, 0);

  if (!Configuration::GetEventFrameworkEnabled()) {
    thread_send_msg(mDispatchThread, enqueueRequest, ctx);
  } else {
    event_call(Configuration::GetDispatchThreadCoreId(),
               enqueueRequest2, ctx, nullptr);
  }

  return ;
}

// called by index thread
void RAIDController::ExecuteIndex(RequestContext *ctx) {
  ctx->ctrl = this;
  uint32_t blockSize = Configuration::GetBlockSize();
  if (ctx->req_type == 'W') {
    mNumIndexWrites++;
  } else {
    mNumIndexReads++;
  }
  for (auto& it : ctx->associatedRequests) {
    RequestContext* req = it.first;
    uint64_t lba = req->lba + it.second.first * blockSize;
    uint64_t blkLba = lba / mMappingBlockUnitSize;
    mWaitIndexQueue[blkLba].insert(it.first);
    double timeNow = GetTimestampInUs();
    if (!mWaitIndexLbaToTime.count(blkLba)) {
      mWaitIndexLbaToTime[blkLba] = timeNow;
      mWaitIndexTimeToLba[timeNow] = blkLba;
    }

    // debug
    mWaitIndexToTime[it.first] = timeNow;
  }
  ctx->associatedRequests.clear();
  if (ctx->ctrl == nullptr) {
    debug_error("ctx %p ctrl is empty!\n", ctx);
    assert(0);
  }

  if (!Configuration::GetEventFrameworkEnabled()) {
    thread_send_msg(mDispatchThread, enqueueIndexRequest, ctx);
  } else {
    event_call(Configuration::GetDispatchThreadCoreId(),
               enqueueIndexRequest2, ctx, nullptr);
  }
}

void RAIDController::EnqueueWrite(RequestContext *ctx)
{
  mWriteQueue.push(ctx);
}

void RAIDController::EnqueueZoneWrite(RequestContext *ctx)
{
  Zone* zone = ctx->zone;
  // in number of blocks
  // zoneId is actually the device ID
//  mIoZoneWriteMap[ctx->zoneId][ctx->zone->GetSlba() + ctx->offset] = ctx;
  mIoZoneWriteMap[ctx->zone][ctx->offset] = ctx;
}

void RAIDController::EnqueueReadPrepare(RequestContext *ctx)
{
  mReadPrepareQueue.push(ctx);
}

void RAIDController::EnqueueReadReaping(RequestContext *ctx)
{
  mReadReapingQueue.push(ctx);
}

void RAIDController::EnqueueWriteIndex(RequestContext *ctx)
{
  mWriteIndexQueue.push(ctx);
}

void RAIDController::EnqueueReadIndex(RequestContext* ctx)
{
  mReadIndexQueue.push(ctx);
}

// index thread
void RAIDController::EnqueueWaitIndex(RequestContext* ctx, uint64_t l) 
{
  uint64_t blkLba = l / mMappingBlockUnitSize;
  mWaitIndexQueue[blkLba].insert(ctx);
  double timeNow = GetTimestampInUs();
  if (!mWaitIndexLbaToTime.count(blkLba)) {
    mWaitIndexLbaToTime[blkLba] = timeNow;
    mWaitIndexTimeToLba[timeNow] = blkLba;
  }

  mWaitIndexToTime[ctx] = timeNow;
}

// index thread
void RAIDController::RemoveWaitIndex(RequestContext* ctx, uint64_t l)
{
  uint64_t k = l / mMappingBlockUnitSize;
  if (mWaitIndexQueue.count(k) && mWaitIndexQueue[k].find(ctx) != mWaitIndexQueue[k].end()) {
    mWaitIndexQueue[k].erase(ctx);
    mWaitIndexToTime.erase(ctx);
    if (mWaitIndexQueue[k].empty()) {
      double time = mWaitIndexLbaToTime[k];
      mWaitIndexLbaToTime.erase(k);
      mWaitIndexTimeToLba.erase(time);
      mWaitIndexQueue.erase(k);
    }
  }
}

uint64_t RAIDController::GetWaitIndexSize() {
  uint64_t s = 0;
  for (auto& it : mWaitIndexQueue) {
    s += it.second.size();
  }
  return s;
}

void RAIDController::PrintWaitIndexTime(double timeNow) {
  for (auto& it : mWaitIndexQueue) {
    for (auto& ctx : it.second) {
      printf(" --- lba %lu ctx %p time %.3lf\n", it.first, ctx, 
          timeNow - mWaitIndexToTime[ctx]);
    }
  }
}

std::queue<RequestContext*>& RAIDController::GetWriteQueue()
{
  return mWriteQueue;
}

//std::map<uint32_t, std::map<uint32_t, RequestContext*>>&
std::map<Zone*, std::map<uint32_t, RequestContext*>>&
RAIDController::GetPendingZoneWriteQueue()
{
  return mIoZoneWriteMap;
}

std::queue<RequestContext*>& RAIDController::GetReadPrepareQueue()
{
  return mReadPrepareQueue;
}

std::queue<RequestContext*>& RAIDController::GetReadReapingQueue()
{
  return mReadReapingQueue;
}

std::queue<RequestContext*>& RAIDController::GetWriteIndexQueue()
{
  return mWriteIndexQueue;
}

std::queue<RequestContext*>& RAIDController::GetReadIndexQueue()
{
  return mReadIndexQueue;
}

// not for GC, but for normal writes
void RAIDController::WriteInDispatchThread(RequestContext *ctx)
{
  // if using mapping blocks, need to reserve one more segment.
  if (mAvailableStorageSpaceInSegments <= 1 + (!mAddressMap->IsAllInMemory())) {
    if (!Configuration::GetEnableGc()) {
      debug_e("gc disabled but no available space!");
      assert(0);
    }

    // when there are no more available segments, only index blocks can write 
    if (mAvailableStorageSpaceInSegments <= 0) {
      if (ctx->type != INDEX) {
        return;
      } else {
//        printf("writing index block to empty block: lba %lu size %u\n",
//            ctx->lba, ctx->size);
      }
      // continue. Find an available segment
    } 

    if (ctx->type != INDEX) {
      return;
    }
  }

  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t curOffset = ctx->curOffset;
  uint32_t size = ctx->size;
  uint32_t numBlocks = size / blockSize;

  if (curOffset == 0 && ctx->timestamp == ~0ull) {
    ctx->timestamp = ++mGlobalTimestamp;
    ctx->pbaArray.resize(numBlocks);
  }

  uint32_t pos = ctx->curOffset;  // within the request
  uint32_t left = ctx->size - ctx->curOffset * blockSize;
  uint32_t last_left = left;

  if (Configuration::StuckDebugMode()) {
    debug_error("pos %u numBlocks %u\n", pos, numBlocks);
  }

  struct timeval s, e;
  int orderedSegId[8], idx = 0;

  for (; pos < numBlocks; ) {
    uint32_t openGroupId = 0;
    uint32_t success_bytes = 0;
    // check whether it has matched segments

    gettimeofday(&s, 0);
    // TODO test!
//    orderedSegId[0] = 0;
//    idx = 1;
    GenerateSegmentWriteOrder(left, orderedSegId, idx);
    gettimeofday(&e, 0);
    MarkDispatchThreadTestTime(s, e);
    assert(idx > 0);

    for (int i = 0; i < idx; i++) {
      uint32_t openGroupId = orderedSegId[i];

      // can not append in two cases: (1) full; (2) state transition
      success_bytes = mOpenSegments[openGroupId]->BufferedAppend(ctx, pos, left);
      mSpIdWriteSizes[openGroupId] += success_bytes;
      if (success_bytes == 0) {
        debug_info("success_bytes: %u\n", success_bytes);
      } else {
        debug_warn("success_bytes: %u, left %u\n", success_bytes, left);
      }
      if (mOpenSegments[openGroupId]->IsFull()) {
        mSegmentsToSeal.emplace_back(mOpenSegments[openGroupId]);
        mOpenSegments[openGroupId] = nullptr;
        if (mNumCurrentOpenSegments < mNumOpenSegmentsThres) {
          createSegmentIfNeeded(&mOpenSegments[openGroupId], openGroupId);
        } else {
          mNotOpenForFullOpenZones[openGroupId] = true;
        }
      }
      left -= success_bytes;
      pos += success_bytes / blockSize;
      if (success_bytes > 0) {
        // do the check again 
        break;
      }
//      openGroupId = (openGroupId + 1) % mNumOpenSegments;
    }

    if (last_left == left) {
      break; // Nothing is written, wait for the next round
    }
    last_left = left;
  }

  ctx->curOffset = pos;
}

// called by index thread
IndexMapStatus RAIDController::LookupIndex(uint64_t lba, PhysicalAddr *pba)
{
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t pbaInBlk = 0;
  IndexMapStatus s = mAddressMap->GetPba(lba / blockSize, pbaInBlk);
  if (s == kHit) {
//    printf("get pba: lba %lu pba %u %u\n", lba, pbaInBlk, mAddressMapMemory[lba / blockSize]);
//    if (mAddressMapMemory[lba / blockSize] != pbaInBlk) {
//      debug_error("index error: lba %lu should be %u but %u\n", lba,
//          mAddressMapMemory[lba / blockSize], pbaInBlk);
//      assert(0);
//    }
  }
  if (s != kHit) {
    if (s == kNeedEvict || s == kNeedRead || s == kFetching) {
      return s;
    } else {
      debug_error("s = %d\n", s);
      assert(0);
    }
  }

  if (pbaInBlk == ~0u) {
    pba->segment = nullptr;
  } else {
    std::shared_lock<std::shared_mutex> r_lock(mSegmentTableMutex);
    pba->segment = mZoneToSegmentMap[pbaInBlk / mZoneSize];
    assert(pba->segment != nullptr);
    pba->zoneId = pbaInBlk / mZoneSize / mTotalNumSegments;
    pba->offset = pbaInBlk % mZoneSize;
  }
  return kHit;
}

void RAIDController::ReadInDispatchThread(RequestContext *ctx)
{
  debug_warn("ctx->status %d PREPARE %d REAPING %d offset %lu size %u\n",
      (int)ctx->status, READ_PREPARE, READ_REAPING, ctx->lba, ctx->size);
  uint64_t slba = ctx->lba;
  int size = ctx->size;
  void *data = ctx->data;
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t numBlocks = size / blockSize;

  if (ctx->status == READ_PREPARE) {
    if (mGcTask.stage != INDEX_UPDATE_COMPLETE) {
      // For any reads that may read the input segment,
      // track its progress such that the GC will
      // not delete input segment before they finishes
      mReadsInCurrentGcEpoch.insert(ctx);
    }

    debug_warn("read prepare %lu %u\n", ctx->lba, ctx->size);
    ctx->status = READ_INDEX_QUERYING;
    ctx->pbaArray.resize(numBlocks);
    ctx->curOffset = 0;
    if (!Configuration::GetEventFrameworkEnabled()) {
      QueryPbaArgs *args = (QueryPbaArgs *)calloc(1, sizeof(QueryPbaArgs));
      args->ctrl = this;
      args->ctx = ctx;
      thread_send_msg(mIndexThread, queryPba, args);
    } else {
      event_call(Configuration::GetIndexThreadCoreId(),
                 queryPba2, this, ctx);
    }
  } else if (ctx->status == READ_REAPING) {
    uint32_t i = ctx->curOffset;
    // an empty segment
    PhysicalAddr prevAddr;
    prevAddr.segment = nullptr;
    uint32_t curSize = 0;

    for (; i < numBlocks; ++i) {
      Segment *segment = ctx->pbaArray[i].segment;
      if (segment == nullptr) {
        uint8_t *block = (uint8_t *)data + i * Configuration::GetBlockSize();
        memset(block, 0, blockSize);
        ctx->successBytes += blockSize;
        if (ctx->successBytes == ctx->targetBytes) {
          ctx->Queue();
        }
        // set the read to an empty segment
        prevAddr.segment = nullptr;
        curSize = 0;
      } else {
        // next segment is different 
        PhysicalAddr& thisAddr = ctx->pbaArray[i];
        if (prevAddr.segment == nullptr) {
          prevAddr = thisAddr;
        }
        curSize += blockSize;
        // concat the blocks in the same segment
        if (i == numBlocks - 1 || (i+1 < numBlocks && 
              (ctx->pbaArray[i+1].segment != prevAddr.segment ||
               ctx->pbaArray[i+1].zoneId != prevAddr.zoneId ||
               ctx->pbaArray[i+1].offset != thisAddr.offset + 1))) {
          uint32_t pos = i - (curSize / blockSize - 1);
          uint32_t unitSizeInBlks = Configuration::GetMaxStripeUnitSize() / blockSize;
          if (!segment->Read(ctx, pos, prevAddr, curSize)) {
            // revert the offset of read
            i = i - (curSize / blockSize) + 1;
            break;
          }
          curSize = 0;

          if (i+1 < numBlocks) {
            prevAddr = ctx->pbaArray[i+1];
          }
        }
      }
    }

    ctx->curOffset = i;
  } else if (ctx->status == READ_INDEX_REAPING) {
    // actually very similar to READ_REAPING. Can merge together
    assert(ctx->curOffset == 0 && ctx->size == blockSize);
    uint32_t i = ctx->curOffset;
    // an empty segment
    uint32_t curSize = 0;
    if (ctx->pbaArray.size() < numBlocks) { 
      ctx->pbaArray.resize(numBlocks);
    }

    for (; i < numBlocks; ++i) {
      auto& pba = ctx->pbaArray[i];
      if (pba.segment == nullptr) {
        assert(ctx->pbaInBlk != ~0ull);
        std::shared_lock<std::shared_mutex> r_lock(mSegmentTableMutex);
        pba.segment = mZoneToSegmentMap[ctx->pbaInBlk / mZoneSize];
        pba.zoneId = ctx->pbaInBlk / mZoneSize / mTotalNumSegments;
        pba.offset = ctx->pbaInBlk % mZoneSize;
        if (pba.segment == nullptr) {
          debug_error("read from invalid segment: zoneId %u offset %u\n",
              pba.zoneId, pba.offset);
          assert(0);
        }
      }

      Segment *segment = pba.segment;
      // next segment is different 
      curSize += blockSize;
      // concat the blocks in the same segment
      if (!segment->Read(ctx, i, pba, curSize)) {
        break;
      }
    }

    ctx->curOffset = i;
  }
}

RequestContext* RAIDController::PrepareIndexContext(IndexMapStatus s,
    uint64_t lba) 
{
  static int cnt = 0;
  static bool stuckPrint = 0;
  RequestContext* ctx = getContextForIndex(); 
  assert(ctx->available == false);
  if (ctx == nullptr) {
    debug_e("no available context for index\n");
    assert(0);
  }
  mAddressMap->FillContext(ctx, s, lba);
  if (Configuration::StuckDebugMode() && stuckPrint == 0) {
    printf("prepare index context %p lba %lu\n", ctx, lba);
  }

  if (ctx->status == WRITE_INDEX_COMPLETE) {
    // no need to write (directly evict and delete the block)
    // directly read
    s = kNeedRead;
    // now it will prepare the read block by itself
    mAddressMap->FillContext(ctx, s, lba);
  } else if (ctx->status == WRITE_INDEX_WAITING) {
    // too many blocks are undergoing evictions. Wait for a while
    ctx->available = true;
    ctx = nullptr;
  }
  if (Configuration::StuckDebugMode() && stuckPrint == 0) {
    printf("nullptr\n");
    stuckPrint = 1;
  }
  return ctx;
}

// index thread
void RAIDController::HandleIndexContext(RequestContext* ctx) {
  PhysicalAddr pba = ctx->pbaArray[0];
  uint32_t blockSize = Configuration::GetBlockSize();
  if (ctx->status == WRITE_INDEX_REAPING) {
    // evicting
    std::shared_lock<std::shared_mutex> r_lock(mSegmentTableMutex);
    auto segId = pba.segment->GetSegmentId();
    if (mSegIdToZoneIdMap.count(segId) == 0) {
      assert(0);
    }
    if (mSegIdToZoneIdMap[segId].size() != mN) {
      assert(0);
    }
    ctx->pbaInBlk = pba.zoneId * mTotalNumSegments * mZoneSize +
      (uint64_t)mSegIdToZoneIdMap[segId][pba.zoneId] * mZoneSize +
      pba.offset;
    ctx->status = WRITE_INDEX_COMPLETE;
    mNumIndexWritesHandled++;
  } else if (ctx->status == READ_INDEX_REAPING) {
    // fetching
    ctx->status = READ_INDEX_COMPLETE;
    mNumIndexReadsHandled++;
  }
  uint32_t oldPbaInBlk = ~0u;
  // you need to invalidate the old mapping block
  IndexMapStatus s = mAddressMap->HandleContext(ctx, oldPbaInBlk);
  ctx->available = true;

  // invalidate the old 
  assert(oldPbaInBlk == ~0u);

  // the context processing can stop here. Now we start to process the user requests
  // restart some requests
  std::set<RequestContext*> ctxToContinue;
  if (s == kEvictFailed) {
    // the block becomes dirty when eviction happens. 
    // invalidate the new mapping block.
    pba.segment->InvalidateBlock(pba.zoneId, pba.offset);
  } else if (s == kEvictSuccess) {
    // select some requests to continue
  } else if (s == kReadSuccess || s == kReadFailed) {
    // continue the processing of read or write user requests
    // read failed: GC changes the position of mapping block,
    //    so it needs to read again
    uint64_t lbaInUnit = ctx->lba / mMappingBlockUnitSize;

    // find the corresponding user request 
    if (mWaitIndexQueue.count(lbaInUnit)) {
      // a copy of the context set
      for (auto& it : mWaitIndexQueue[lbaInUnit]) {
        ctxToContinue.insert(it);
        if (ctxToContinue.size() >= 4) break;
      }
    }
  }

  // find some requests to continue
  double timeNow = GetTimestampInUs();
  if (ctxToContinue.size() < 4) {
    for (auto& it : mWaitIndexTimeToLba) {
      if (timeNow - it.first > 20.0 && !Configuration::StuckDebugMode()) {
        debug_error("request stuck for %.3lf seconds\n", timeNow - it.first);
        Configuration::SetStuckDebugMode(timeNow);
      }
      uint64_t blkLba = it.second;
      assert(mWaitIndexQueue.count(blkLba));
      for (auto& it1 : mWaitIndexQueue[blkLba]) {
        ctxToContinue.insert(it1);
        if (ctxToContinue.size() >= 4) break;
      }
      if (ctxToContinue.size() >= 4) break;
    }
  }

  for (auto& it : ctxToContinue) {
    if (it->status == WRITE_INDEX_UPDATING) {
      updatePba2(this, it);
    } else if (it->status == READ_INDEX_QUERYING) {
      queryPba2(this, it);
    } else {
      debug_error("status %d\n", it->status);
      assert(0);
    }
  }
}

uint64_t RAIDController::rescheduleIndexTask() {
  std::set<RequestContext*> ctxToContinue;

  for (auto& it : mWaitIndexTimeToLba) {
    uint64_t blkLba = it.second;
    assert(mWaitIndexQueue.count(blkLba));
    for (auto& it1 : mWaitIndexQueue[blkLba]) {
      ctxToContinue.insert(it1);
      if (ctxToContinue.size() >= 4) break;
    }
    if (ctxToContinue.size() >= 4) break;
  }

  for (auto& it : ctxToContinue) {
    if (it->status == WRITE_INDEX_UPDATING) {
      updatePba2(this, it);
    } else if (it->status == READ_INDEX_QUERYING) {
      queryPba2(this, it);
    } else {
      it->printMsg();
      debug_error("status %d ctx %p\n", it->status, it);
      assert(0);
    }
  }

  return ctxToContinue.size();
//  return 0;
}

bool RAIDController::scheduleGc()
{
  if (mAvailableStorageSpaceInSegments > mStorageSpaceThresholdForGcInSegments) {
    return false;
  }
  printf("schedule GC. Available storage: %u %u\n",
         mAvailableStorageSpaceInSegments,
         mStorageSpaceThresholdForGcInSegments);

  // Use Greedy algorithm to pick segments
  std::vector<Segment*> groups;
  for (Segment *segment : mSealedSegments) {
      groups.emplace_back(segment);
  }
  if (groups.size() == 0) {
    return false;
  }
  std::sort(groups.begin(), groups.end(), [](const Segment *lhs, const Segment *rhs) {
      uint32_t uselessBlocks1 = lhs->GetFullNumBlocks() - (lhs->GetNumBlocks() - lhs->GetNumInvalidBlocks());
      uint32_t uselessBlocks2 = rhs->GetFullNumBlocks() - (rhs->GetNumBlocks() - rhs->GetNumInvalidBlocks());
      double score1 = (double)uselessBlocks1 / lhs->GetFullNumBlocks();
      double score2 = (double)uselessBlocks2 / rhs->GetFullNumBlocks(); 
      return score1 > score2;
      });

  mGcTask.inputSegment = groups[0];
  {
    uint32_t uselessBlocks = mGcTask.inputSegment->GetFullNumBlocks() - 
      (mGcTask.inputSegment->GetNumBlocks() - mGcTask.inputSegment->GetNumInvalidBlocks());
    printf("Select: %p (%d), Score: %f = %u/%u\n", 
        mGcTask.inputSegment, mGcTask.inputSegment->GetSegmentId(),
        (double)uselessBlocks / groups[0]->GetFullNumBlocks(),
        uselessBlocks, groups[0]->GetFullNumBlocks());
  }

  mGcTask.maxZoneId = mN;
  mGcTask.maxOffset = mHeaderRegionSize + mDataRegionSize;
  mGcTask.readUnitSize = mGcTask.inputSegment->GetStripeUnitSize();

  printf("[GC start] Schedule GC. Available storage: %u %u. "
      "index r %lu (%lu) w %lu (%lu)\n",
         mAvailableStorageSpaceInSegments,
         mStorageSpaceThresholdForGcInSegments,
         mNumIndexReads.load(), mNumIndexReadsHandled.load(),
         mNumIndexWrites.load(), mNumIndexWritesHandled.load());

  return true;
}

bool RAIDController::ProceedGc()
{
  bool hasProgress = false;
  if (!Configuration::GetEnableGc()) {
    return hasProgress;
  }

  if (mGcTask.stage == IDLE) { // IDLE
    if (scheduleGc()) {
      hasProgress = true;
      mGcTask.stage = INIT;
    }
  }

  if (mGcTask.stage == INIT) {
    initializeGcTask();
  }

  if (mGcTask.stage == REWRITING) {
    hasProgress |= progressGcWriter();
    hasProgress |= progressGcReader();

//    if (mGcTask.curZoneId == mGcTask.maxZoneId)
    if (mGcTask.nextOffset == mGcTask.maxOffset) {
      assert(mGcTask.numWriteSubmitted <= mGcTask.numReads);
      assert(mGcTask.numWriteFinish <= mGcTask.numWriteSubmitted);
      if (mGcTask.numWriteSubmitted == mGcTask.numReads
          && mGcTask.numWriteFinish == mGcTask.numWriteSubmitted) {
        printf("[GC min] rewrite complete: numReads %u numWrites %u numFinish %u "
            "numUnitReads %u numUnitWrites %u mapping size %lu\n",
            mGcTask.numReads, mGcTask.numWriteSubmitted,
            mGcTask.numWriteFinish, mGcTask.numUnitReads,
            mGcTask.numUnitWrites, mGcTask.mappings.size());
        if (mGcTask.mappings.size() < mGcTask.numWriteFinish) {
          printf("[ERROR] mapping size %lu numWriteFinish %u\n",
              mGcTask.mappings.size(), mGcTask.numWriteFinish);
        }
        assert(mGcTask.mappings.size() <= mGcTask.numWriteFinish);
        mGcTask.stage = REWRITE_COMPLETE;
      }
    }
  } 
  
  if (mGcTask.stage == REWRITE_COMPLETE) {
    hasProgress = true;
    mGcTask.stage = INDEX_UPDATING;

    if (!Configuration::GetEventFrameworkEnabled()) {
      thread_send_msg(mIndexThread, progressGcIndexUpdate, this);
    } else {
      event_call(Configuration::GetIndexThreadCoreId(),
          progressGcIndexUpdate2, this, nullptr);
    }
  }

  if (mGcTask.stage == INDEX_UPDATING_BATCH) {
    if (mGcTask.mappings.size() != 0) {
      hasProgress = true;
      mGcTask.stage = INDEX_UPDATING;
      if (!Configuration::GetEventFrameworkEnabled()) {
        thread_send_msg(mIndexThread, progressGcIndexUpdate, this);
      } else {
        event_call(Configuration::GetIndexThreadCoreId(),
            progressGcIndexUpdate2, this, nullptr);
      }
    } else { // Finish updating all mappings
      hasProgress = true;
      mGcTask.stage = INDEX_UPDATE_COMPLETE;
    }
  }

  if (mGcTask.stage == INDEX_UPDATE_COMPLETE) {
    if (mReadsInCurrentGcEpoch.empty()) {
      hasProgress = true;
      mGcTask.inputSegment->Reset(nullptr);
      mGcTask.stage = RESETTING_INPUT_SEGMENT;
    }
  }

  if (mGcTask.stage == RESETTING_INPUT_SEGMENT) {
    if (mGcTask.inputSegment->IsResetDone()) {
      auto zones = mGcTask.inputSegment->GetZones();
      for (uint32_t i = 0; i < zones.size(); ++i) {
        mDevices[i]->ReturnZone(zones[i]);
      }
      // remove segment
      mSealedSegments.erase(mGcTask.inputSegment);
      std::scoped_lock<std::shared_mutex> w_lock(mSegmentTableMutex);
      uint32_t segId = mGcTask.inputSegment->GetSegmentId();
      for (uint32_t j = 0; j < mSegIdToZoneIdMap[segId].size(); j++) {
        uint32_t it = mSegIdToZoneIdMap[segId][j];
        mZoneToSegmentMap[j * mTotalNumSegments + it] = nullptr;
      }
      mSegIdToZoneIdMap.erase(segId);
      delete mGcTask.inputSegment;
      mOpenSegments[mNumOpenSegments - 1]->unsetForGc();
      mAvailableStorageSpaceInSegments += 1;
      mGcTask.stage = IDLE;
      printf("[GC fin] complete index r %lu (%lu) w %lu (%lu)\n",
          mNumIndexReads.load(), mNumIndexReadsHandled.load(), 
          mNumIndexWrites.load(), mNumIndexWritesHandled.load());

      // start creating segments after GC if needed
      for (uint32_t i = 0; i < mOpenSegments.size(); ++i) {
        createSegmentIfNeeded(&mOpenSegments[i], i);
      }
    }
  }

  return hasProgress;
}

void RAIDController::Drain()
{
  printf("Perform draining on the system.\n");
  DrainArgs args;
  args.ctrl = this;
  args.success = false;
  while (!args.success) {
    args.ready = false;
    thread_send_msg(mDispatchThread, tryDrainController, &args);
    busyWait(&args.ready);
  }
}

std::queue<RequestContext*>& RAIDController::GetEventsToDispatch()
{
  return mEventsToDispatch;
}

void RAIDController::EnqueueEvent(RequestContext *ctx)
{
  mEventsToDispatch.push(ctx);
}

int RAIDController::GetNumInflightRequests()
{
  return mInflightUserRequestContext.size();
}

bool RAIDController::ExistsGc()
{
  return mGcTask.stage != IDLE;
}

void RAIDController::createSegmentIfNeeded(Segment **segment, uint32_t spId)
{
  if (*segment != nullptr) return;
  if (mAvailableStorageSpaceInSegments == 1) {
    printf("only one available segment\n");
  }
  // Check there are available zones
  if (mAvailableStorageSpaceInSegments == 0) {
    if (mGcTask.stage == INDEX_UPDATING || 
        mGcTask.stage == INDEX_UPDATING_BATCH || 
        mGcTask.stage == INDEX_UPDATE_COMPLETE ||
        mGcTask.stage == RESETTING_INPUT_SEGMENT) {
//      printf("GC is running, wait for GC to finish\n");
      return;
    }
    assert(0);
    printf("No available storage; this should never happen!\n");
    return ;
  }

  mNumCurrentOpenSegments++;
  mAvailableStorageSpaceInSegments -= 1;
  Segment *seg = new Segment(this, mNextAssignedSegmentId++,
                             mRequestContextPoolForSegments, mReadContextPool,
                             mStripeWriteContextPools[spId], spId);
  for (uint32_t i = 0; i < mN; ++i) {
    std::scoped_lock<std::shared_mutex> w_lock(mSegmentTableMutex);
    Zone* zone = mDevices[i]->OpenZone();
    if (zone == nullptr) {
      printf("No available zone in device %d, storage space is exhuasted!\n", i);
      assert(0);
    }
    seg->AddZone(zone);
    uint32_t zoneId = zone->GetSlba() / zone->GetSize();
    mZoneToSegmentMap[i * mTotalNumSegments + zoneId] = seg;
    mSegIdToZoneIdMap[seg->GetSegmentId()].push_back(zoneId);
  }

  if (Configuration::GetSystemMode() == RAIZN_SIMPLE) {
    // add meta zone for each device
    for (uint32_t i = 0; i < mN; ++i) { 
      Zone* zone = mDevices[i]->OpenZone();
      if (zone == nullptr) {
        printf("No available zone in device %d, storage space is exhuasted!\n", i);
        assert(0);
      }
      debug_warn("add meta zone %p\n", zone); 
      seg->AddMetaZone(zone);
    }
  }

  if (spId == mNumOpenSegments + 1) {
    printf("Create spare segment %p\n", seg);
  } else {
    printf("Create normal segment %p spId %u segid %u\n", seg, spId,
        seg->GetSegmentId());
  }
  seg->FinalizeCreation();
  *segment = seg;
}

std::queue<RequestContext*>& RAIDController::GetRequestQueue()
{
  return mRequestQueue;
}

std::mutex& RAIDController::GetRequestQueueMutex()
{
  return mRequestQueueMutex;
}

struct spdk_thread *RAIDController::GetIoThread(int id)
{
  return mIoThread[id].thread;
}

struct spdk_thread *RAIDController::GetDispatchThread()
{
  return mDispatchThread;
}

struct spdk_thread *RAIDController::GetEcThread()
{
  return mEcThread;
}

struct spdk_thread *RAIDController::GetIndexThread()
{
  return mIndexThread;
}

struct spdk_thread *RAIDController::GetCompletionThread()
{
  return mCompletionThread;
}

void RAIDController::initializeGcTask()
{
  mGcTask.curZoneId = 0;
  mGcTask.nextOffset = mHeaderRegionSize;
  mGcTask.stage = REWRITING;

  mGcTask.writerPos = 0;
  mGcTask.readerPos = 0;

  mGcTask.numWriteSubmitted = 0;
  mGcTask.numWriteFinish = 0;
  mGcTask.numReads = 0;
  mGcTask.numUnitReads = 0;
  mGcTask.numUnitWrites = 0;

  mGcTask.mappings.clear();
  mGcTask.debugMessages.clear();
  mGcTask.debugInvalidated = 0;

  uint32_t unitSize = mGcTask.inputSegment->GetStripeUnitSize();
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t metaSize = Configuration::GetMetadataSize();

  // Initialize the status of the context pool
  for (uint32_t i = 0; i < mGcTask.numBuffers; ++i) {
    mGcTask.contextPool[i].Clear();
    mGcTask.contextPool[i].available = true;
    mGcTask.contextPool[i].ctrl = this;
    mGcTask.contextPool[i].lbaArray.resize(unitSize / Configuration::GetBlockSize());
    mGcTask.contextPool[i].pbaArray.resize(unitSize / Configuration::GetBlockSize());
    mGcTask.contextPool[i].gcTask = &mGcTask;
    mGcTask.contextPool[i].type = GC;
    mGcTask.contextPool[i].lba = ~0ull;
    mGcTask.contextPool[i].data = (uint8_t*)mGcTask.dataBuffer + i * unitSize;
    mGcTask.contextPool[i].meta = (uint8_t*)mGcTask.metaBuffer + i * unitSize / blockSize * metaSize;
    mGcTask.contextPool[i].targetBytes = unitSize;
    mGcTask.contextPool[i].status = WRITE_COMPLETE;
  }
}

bool RAIDController::progressGcReader()
{
  bool hasProgress = false;
  uint32_t blockSize = Configuration::GetBlockSize();
  // Find contexts that are available, schedule read for valid blocks
  RequestContext *nextReader = &mGcTask.contextPool[mGcTask.readerPos];

  while (nextReader->available && nextReader->status == WRITE_COMPLETE) {
    if (nextReader->lba != ~0ull) {
      // The sign of valid lba means a successful rewrite a valid block
      // So we update the information here
      nextReader->curOffset = 0;
      mGcTask.numWriteFinish += nextReader->nValid;
      for (int i = 0; i < nextReader->nValid; i++) {
        uint64_t lba = nextReader->lbaArray[i];
        assert(lba != ~0ull);
        mGcTask.mappings[lba].second = nextReader->pbaArray[i];
      }
    }

    // at the end of GC, it will eventually invalidate all the context
    nextReader->available = false;
    nextReader->lba = 0;

    bool success = true;
//    if (mGcTask.curZoneId != mGcTask.maxZoneId) 
    if (mGcTask.nextOffset < mGcTask.maxOffset) 
    {
      nextReader->req_type = 'R';
      nextReader->status = READ_REAPING;
      nextReader->successBytes = 0;
      nextReader->targetBytes = std::min(mGcTask.readUnitSize, 
          (mGcTask.maxOffset - mGcTask.nextOffset) * blockSize);

      do {
        nextReader->segment = mGcTask.inputSegment;
        nextReader->zoneId = mGcTask.curZoneId;
        nextReader->offset = mGcTask.nextOffset;
        nextReader->UpdateContinousPba();

        success = mGcTask.inputSegment->ReadValid(nextReader, 0, nextReader->GetPba(), nextReader->targetBytes);
        if (!success) break;

        mGcTask.curZoneId += 1;
        // scan data across devices instead of one zone at a time
        if (mGcTask.curZoneId == mGcTask.maxZoneId) {
          mGcTask.nextOffset += nextReader->targetBytes / blockSize;
          mGcTask.curZoneId = 0;
        } 
      } 
      while (!nextReader->nValid && mGcTask.nextOffset < mGcTask.maxOffset);
      mGcTask.numReads += nextReader->nValid;
      if (nextReader->nValid) {
        mGcTask.numUnitReads++;
      }
    }
    if (!success) {
      // will retry later. Revert the state
      if (nextReader->nValid) {
        mGcTask.numReads -= nextReader->nValid;
        mGcTask.numUnitReads--;
      }

      nextReader->status = WRITE_COMPLETE;
      nextReader->available = true;
      nextReader->lba = ~0ull;
      break;
    }
    hasProgress = true;
    mGcTask.readerPos = (mGcTask.readerPos + 1) % mGcTask.numBuffers;
    nextReader = &mGcTask.contextPool[mGcTask.readerPos];
  }

  return hasProgress;
}

bool RAIDController::progressGcWriter()
{
  bool hasProgress = false;
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t metaSize = Configuration::GetMetadataSize();
  // Process blocks that are read and valid, and rewrite them 
  RequestContext *nextWriter = &mGcTask.contextPool[mGcTask.writerPos];

  while ((nextWriter->available && nextWriter->status == READ_COMPLETE)
      || (nextWriter->available == false && 
        nextWriter->status == WRITE_REAPING && 
        nextWriter->curOffset < nextWriter->targetBytes / blockSize)) {
    assert(nextWriter->nValid != 0);

    if (nextWriter->status == READ_COMPLETE) {
      nextWriter->size = 0;
      for (int i = 0; i < nextWriter->targetBytes / blockSize; i++) {
        uint32_t pos = nextWriter->size / blockSize;
        if (nextWriter->validBits & (1ull << i)) {
          // valid block, copy
          if (i != pos) {
            memcpy(nextWriter->data + nextWriter->size,
              nextWriter->data + i * blockSize, blockSize);
            memcpy(nextWriter->meta + nextWriter->size / blockSize * metaSize,
              nextWriter->meta + i * metaSize, metaSize); 
            nextWriter->lbaArray[pos] = nextWriter->lbaArray[i];
            nextWriter->pbaArray[pos] = nextWriter->pbaArray[i];
          }
          nextWriter->size += blockSize;
        }
      }
      
      assert(nextWriter->size == nextWriter->nValid * blockSize);
      nextWriter->backupPbaArray = nextWriter->pbaArray;
      nextWriter->req_type = 'W';
      nextWriter->status = WRITE_REAPING;
      nextWriter->curOffset = 0;
      nextWriter->successBytes = 0;
      nextWriter->available = false;
      nextWriter->targetBytes = nextWriter->size;
      nextWriter->timestamp = ((BlockMetadata *)nextWriter->meta)->fields.coded.timestamp;
    }

    uint32_t prevPos = nextWriter->curOffset;
    uint32_t pos = nextWriter->curOffset;
    uint32_t numBlocks = nextWriter->size / blockSize;
    uint32_t left = (numBlocks - pos) * blockSize;

    while (pos < numBlocks) {
      uint32_t success_bytes = 0;
      for (uint32_t i = mNumOpenSegments - 1; i < mNumOpenSegments; i += 1) {
        uint64_t segId = 0;
        if (mOpenSegments[i] == nullptr) {
          Configuration::SetPossibleStuckReason("no open segment in gc");
          break; // wait for sealing segments 
        }
        if (mNumOpenSegments > 1) {
          mOpenSegments[i]->setForGc();
        }

        success_bytes = mOpenSegments[i]->BufferedAppend(nextWriter, pos, left);
        if (mOpenSegments[i]->IsFull()) {
          mSegmentsToSeal.emplace_back(mOpenSegments[i]);
          mOpenSegments[i] = nullptr;
          if (mNumCurrentOpenSegments >= mNumOpenSegmentsThres) {
            mNotOpenForFullOpenZones[i] = true;
            break;
          }
          createSegmentIfNeeded(&mOpenSegments[i], i);
        }
        left -= success_bytes;
        pos += success_bytes / blockSize;
        if (left == 0) {
          break;
        }
      }
      if (success_bytes == 0) {
        break;
      }

      hasProgress = true;

      // prepare mappings for index update 
      for (int i = prevPos; i < pos; i++) {
        uint64_t lba = nextWriter->lbaArray[i];
        PhysicalAddr oldPba = nextWriter->backupPbaArray[i];
        if (mGcTask.mappings.count(lba)) {
          debug_error("[ERROR] lba %lu already exists: old (seg %u, %u, %u)\n", lba,
              mGcTask.mappings[lba].first.segment->GetSegmentId(),
              mGcTask.mappings[lba].first.zoneId, mGcTask.mappings[lba].first.offset);
          printf("[ERROR] lba %lu already exists: old (seg %u, %u, %u) new (seg %u %u %u)\n", lba,
              mGcTask.mappings[lba].first.segment->GetSegmentId(),
              mGcTask.mappings[lba].first.zoneId, mGcTask.mappings[lba].first.offset,
              mGcTask.mappings[lba].second.segment->GetSegmentId(),
              mGcTask.mappings[lba].second.zoneId, mGcTask.mappings[lba].second.offset);
          assert(0);
        }
        mGcTask.mappings[lba] = std::make_pair(oldPba, PhysicalAddr());
        mGcTask.numWriteSubmitted += 1;
      }
      prevPos = pos;
      nextWriter->curOffset = pos;
    }

    if (pos < numBlocks) {
      break;
    }

    mGcTask.numUnitWrites++;
    mGcTask.writerPos = (mGcTask.writerPos + 1) % mGcTask.numBuffers;
    nextWriter = &mGcTask.contextPool[mGcTask.writerPos];
  }
  return hasProgress;
}

void RAIDController::ProgressGcIndexUpdate() {
  std::vector<uint64_t> lbas;
  std::vector<std::pair<PhysicalAddr, PhysicalAddr>> pbas;

  auto it = mGcTask.mappings.begin();
  uint32_t count = 0;
  while (it != mGcTask.mappings.end()) {
    lbas.emplace_back(it->first);
    pbas.emplace_back(it->second);

    it = mGcTask.mappings.erase(it);

    count++;
    if (count == 256) break;
  }
  GcBatchUpdateIndex(lbas, pbas);
  mGcTask.stage = INDEX_UPDATING_BATCH;
}

bool RAIDController::CheckSegments()
{
  bool stateChanged = false;
  for (uint32_t i = 0; i < mOpenSegments.size(); ++i) {
    if (mOpenSegments[i] != nullptr) {
      stateChanged |= mOpenSegments[i]->StateTransition();
      if (mOpenSegments[i]->IsFull()) {
        mSegmentsToSeal.emplace_back(mOpenSegments[i]);
        mOpenSegments[i] = nullptr;
        if (mNumCurrentOpenSegments < mNumOpenSegmentsThres) { 
          // control the number of open segments
          createSegmentIfNeeded(&mOpenSegments[i], i);
        } else {
          mNotOpenForFullOpenZones[i] = true;
        }
      }
    } else if (mOpenSegments[i] == nullptr) { 
      if (mNotOpenForFullOpenZones[i] &&
          mNumCurrentOpenSegments < mNumOpenSegmentsThres) {
        createSegmentIfNeeded(&mOpenSegments[i], i);
        mNotOpenForFullOpenZones[i] = false;
      }
    }
  }

  for (auto it = mSegmentsToSeal.begin();
      it != mSegmentsToSeal.end();
      )
  {
    stateChanged |= (*it)->StateTransition();
    if ((*it)->GetStatus() == SEGMENT_SEALED) {
      mNumCurrentOpenSegments--;
      mSealedSegments.insert(*it);
      it = mSegmentsToSeal.erase(it);
    } else {
      ++it;
    }
  }

  return stateChanged;
}

GcTask* RAIDController::GetGcTask()
{
  return &mGcTask;
}

uint32_t RAIDController::GetHeaderRegionSize()
{
  return mHeaderRegionSize;
}

uint32_t RAIDController::GetDataRegionSize()
{
  return mDataRegionSize;
}

uint32_t RAIDController::GetFooterRegionSize()
{
  return mFooterRegionSize;
}

uint32_t RAIDController::GetZoneSize() {
  return mZoneSize;
}

bool RAIDController::isLargeRequest(uint32_t size) {
  return Configuration::GetLargeRequestThreshold() <= size;
}

void RAIDController::MarkWriteLatency(RequestContext* context) {
  struct timeval tv = context->timeA, tv2 = context->timeB;
  uint32_t latency = (tv2.tv_sec - tv.tv_sec) * 1000000 + (tv2.tv_usec -
      tv.tv_usec);
  mWriteLatCnt[latency]++; 
}

void RAIDController::MarkReadLatency(RequestContext* context) {
  struct timeval tv = context->timeA, tv2 = context->timeB;
  uint32_t latency = (tv2.tv_sec - tv.tv_sec) * 1000000 + (tv2.tv_usec -
      tv.tv_usec);
  mReadLatCnt[latency]++; 
}

void RAIDController::MarkCompletionThreadBufferCopyTime(struct timeval s, struct timeval e) {
  uint64_t timeInUs = (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec);
  mCompletionThreadBufferCopyTime += timeInUs;
}

void RAIDController::MarkCompletionThreadIdleTime(struct timeval s, struct timeval e) {
  uint64_t timeInUs = (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec);
  mCompletionThreadIdleTime += timeInUs;
}

void RAIDController::MarkIoThreadZoneWriteTime(struct timeval s, struct timeval e) {
  uint64_t timeInUs = (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec);
  mIoThreadZoneWriteTime += timeInUs;
}

void RAIDController::MarkIoThreadZoneWriteCompleteTime(struct timeval s, struct timeval e) {
  uint64_t timeInUs = (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec);
  mIoThreadZoneWriteCompleteTime += timeInUs;
}

void RAIDController::MarkIoThreadCheckPendingWritesTime(struct timeval s, struct timeval e) {
  uint64_t timeInUs = (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec);
  mIoThreadCheckPendingWritesTime += timeInUs;
}

void RAIDController::MarkDispatchThreadEnqueueTime(struct timeval s, struct timeval e) {
  uint64_t timeInUs = (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec);
  mDispatchThreadEnqueueTime += timeInUs;
}

void RAIDController::MarkDispatchThreadBackgroundTime(struct timeval s, struct timeval e) {
  uint64_t timeInUs = (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec);
  mDispatchThreadBackgroundTime += timeInUs;
}

void RAIDController::MarkDispatchThreadEventTime(struct timeval s, struct timeval e) {
  uint64_t timeInUs = (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec);
  mDispatchThreadEventTime += timeInUs;
}

void RAIDController::MarkDispatchThreadHandleContextTime(struct timeval s, struct timeval e) {
  uint64_t timeInUs = (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec);
  mDispatchThreadHandleContextTime += timeInUs;
}

void RAIDController::MarkDispatchThreadTestTime(struct timeval s, struct timeval e) {
  uint64_t timeInUs = (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec);
  mDispatchThreadTestTime += timeInUs;
}

void RAIDController::MarkIndexThreadUpdatePbaTime(struct timeval s, struct timeval e) {
  uint64_t timeInUs = (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec);

  mIndexThreadUpdatePbaTime += timeInUs;
}

void RAIDController::MarkIndexThreadQueryPbaTime(struct timeval s, struct timeval e) {
  uint64_t timeInUs = (e.tv_sec - s.tv_sec) * 1000000 + (e.tv_usec - s.tv_usec);

  mIndexThreadQueryPbaTime += timeInUs;
}

void RAIDController::printIoThreadTime(double timeInUs) {
  uint64_t timeWrite = 0, timeCheck = 0, timeComplete = 0;
  timeWrite = mIoThreadZoneWriteTime;
  timeCheck = mIoThreadCheckPendingWritesTime;
  timeComplete = mIoThreadZoneWriteCompleteTime;
  printf("Io thread zone write time %lu (%.2lf %%) us, "
      "check pending writes time %lu (%.2lf %%) us, "
      "zone write complete time %lu (%.2lf %%) \n",
//      timeWrite, (double)timeWrite * 100 / (timeWrite + timeCheck),
//      timeCheck, (double)timeCheck * 100 / (timeWrite + timeCheck)
      timeWrite, (double)timeWrite * 100 / timeInUs,
      timeCheck, (double)timeCheck * 100 / timeInUs,
      timeComplete, (double)timeComplete * 100 / timeInUs
      );
}

void RAIDController::printCompletionThreadTime() {
  uint64_t timeCopy = mCompletionThreadBufferCopyTime;
  uint64_t timeIdle = mCompletionThreadIdleTime;
  printf("Completion thread buffer copy time %lu (%.2lf %%) us, "
      "idle time %lu (%.2lf %%) us\n",
      timeCopy, (double)timeCopy * 100 / (timeCopy + timeIdle),
      timeIdle, (double)timeIdle * 100 / (timeCopy + timeIdle));
}

void RAIDController::printDispatchThreadTime(double timeInUs) {
  uint64_t timeEnqueue = mDispatchThreadEnqueueTime;
  uint64_t timeBackground = mDispatchThreadBackgroundTime;
  uint64_t timeEvent = mDispatchThreadEventTime;
  uint64_t timeTest = mDispatchThreadTestTime;
  uint64_t timeHandleContext = mDispatchThreadHandleContextTime;
  
  printf("Dispatch thread enqueue time %lu (%.2lf %%) us, "
      "background time %lu (%.2lf %%) us, "
      "event time %lu (%.2lf %%, tested %.2lf %%) us, "
      "handle context time %lu (%.2lf %%) us\n",
      timeEnqueue, (double)timeEnqueue * 100 / timeInUs,
      timeBackground, (double)timeBackground * 100 / timeInUs,
      timeEvent, (double)timeEvent * 100 / timeInUs,
      (double)timeTest * 100 / timeInUs,
      timeHandleContext, (double)timeHandleContext * 100 / timeInUs);
}

void RAIDController::printIndexThreadTime(double timeInUs) {
  uint64_t timeUpdatePba = mIndexThreadUpdatePbaTime;
  uint64_t timeQueryPba = mIndexThreadQueryPbaTime;

  printf("Index thread update pba time %lu (%.2lf %%) us, "
      "query pba time %lu (%.2lf %%) us\n",
      timeUpdatePba, (double)timeUpdatePba * 100 / timeInUs,
      timeQueryPba, (double)timeQueryPba * 100 / timeInUs);
}

void RAIDController::GenerateSegmentWriteOrder(uint32_t size, int* res, int& res_id)
{
  int zoneWriteSegid[16], zoneAppendSegid[16];
  int zwIdx = 0, zaIdx = 0;
//  uint64_t zwMinSize = ~0ull, zaMinSize = ~0ull;
//  uint32_t zwSelected = ~0u, zaSelected = ~0u;
  res_id = 0;
  // find the matched segments
  auto thres = Configuration::GetLargeRequestThreshold();
  for (int i = 0; i < mNumOpenSegments; i++) {
    if (mOpenSegments[i] == nullptr) {
      continue;
    }
    if ((thres <= size) ^ mOpenGroupForLarge[i]) {
      continue;
    }
    // accelerate: directly return for in-flight stripes
    if (mOpenSegments[i]->HasInFlightStripe()) {
      res[res_id++] = i;
      return;
    }

    if (mOpenSegments[i]->isUsingAppend()) {
      zoneAppendSegid[zaIdx++] = i;
//      if (mSpIdWriteSizes[i] < zaMinSize) {
//        zaMinSize = mSpIdWriteSizes[i];
//        zaSelected = zaIdx - 1;
//      }
    } else {
      zoneWriteSegid[zwIdx++] = i;
//      if (mSpIdWriteSizes[i] < zwMinSize) {
//        zwMinSize = mSpIdWriteSizes[i];
//        zwSelected = zwIdx - 1;
//      }
    }
  }

  // if no matched segments, find the smallest segment 
  // find some segments that match the request; do not care about the order
  if (zaIdx + zwIdx == 0) {
    uint32_t min_unit = ~0u;
    for (int i = 0; i < mNumOpenSegments; i++) {
      int unit_size = Configuration::GetStripeUnitSize(i);
      if (unit_size < min_unit) {
        zaIdx = zwIdx = 0;
//        zaSelected = zwSelected = ~0u;
//        zwMinSize = zaMinSize = ~0ull;
        min_unit = unit_size;
      }
      if (unit_size == min_unit) {
        if (mOpenSegments[i] == nullptr) {
          continue;
        }
        if (mOpenSegments[i]->isUsingAppend()) {
          zoneAppendSegid[zaIdx++] = i;
//          if (mSpIdWriteSizes[i] < zaMinSize) {
//            zaMinSize = mSpIdWriteSizes[i];
//            zaSelected = zaIdx - 1;
//          }
        } else {
          zoneWriteSegid[zwIdx++] = i;
//          if (mSpIdWriteSizes[i] < zwMinSize) {
//            zwMinSize = mSpIdWriteSizes[i];
//            zwSelected = zwIdx - 1;
//          }
        }
      }
    }

    if (zaIdx + zwIdx == 0) {
      printf("No available segments!\n");
      assert(0);
    }
  }

  // find the stripe with inflight stripe first
//  uint32_t segid_w_inflight_stripe = ~0u;
//  bool foundInZoneWrite = false;
//  for (int i = 0; i < zwIdx; i++) {
//    int it = zoneWriteSegid[i];
//    if (mOpenSegments[it]->HasInFlightStripe()) {
//      segid_w_inflight_stripe = it;
//      foundInZoneWrite = true;
//      break;
//    }
//  }
//
//  for (int i = 0; i < zaIdx; i++) {
//    int it = zoneAppendSegid[i];
//    if (mOpenSegments[it]->HasInFlightStripe()) {
//      segid_w_inflight_stripe = it;
//      break;
//    }
//  }
//
//  // if has inflight, do not care about the order:
//  // Select the inflight segment first, then other same-type segments,
//  // then the opposite-type segments
//  if (segid_w_inflight_stripe != ~0u) {
//    res[res_id++] = segid_w_inflight_stripe;
//    if (foundInZoneWrite) {
//      // push zone-write segments first
//      for (int i = 0; i < zwIdx; i++) {
//        int it = zoneWriteSegid[i];
//        if (it != segid_w_inflight_stripe) {
//          res[res_id++] = it;
//        }
//      }
//      for (int i = 0; i < zaIdx; i++) {
//        int it = zoneAppendSegid[i];
//        res[res_id++] = it;
//      }
//    } else {
//      // push zone-append segments first
//      for (int i = 0; i < zaIdx; i++) {
//        int it = zoneAppendSegid[i];
//        if (it != segid_w_inflight_stripe) {
//          res[res_id++] = it;
//        }
//      }
//      for (int i = 0; i < zwIdx; i++) {
//        int it = zoneWriteSegid[i];
//        res[res_id++] = it;
//      }
//    }
//    return;
//  }

  // do not have inflight stripes. Select the zone-write first
  uint64_t minSize = ~0ull;
  uint32_t selected = ~0u;
  for (int i = 0; i < zwIdx; i++) {
    uint64_t curSize = mSpIdWriteSizes[zoneWriteSegid[i]];
    if (curSize < minSize) {
      minSize = curSize;
      selected = i;
    }
  }

  if (selected != ~0u) {
    // round-robin way, no need to sort all of them
    res[res_id++] = zoneWriteSegid[selected];
    for (int i = (1 + selected) % zwIdx; i != selected; i = (1 + i) % zwIdx) {
      res[res_id++] = zoneWriteSegid[i];
    }
  }

  // then select the zone-append segments 
  minSize = ~0ull;
  selected = ~0u;
  for (int i = 0; i < zaIdx; i++) {
    uint64_t curSize = mSpIdWriteSizes[zoneAppendSegid[i]];
    if (curSize < minSize) {
      minSize = curSize;
      selected = i;
    }
  }

  if (selected != ~0u) {
    // round-robin way, no need to sort all of them
    res[res_id++] = zoneAppendSegid[selected];
    for (int i = (1 + selected) % zaIdx; i != selected; i = (1 + i) % zaIdx) {
      res[res_id++] = zoneAppendSegid[i];
    }
  }
}

void RAIDController::RemoveRequestFromGcEpochIfNecessary(RequestContext *ctx)
{
  if (mReadsInCurrentGcEpoch.empty()) {
    return;
  }

  if (mReadsInCurrentGcEpoch.find(ctx) != mReadsInCurrentGcEpoch.end()) {
    mReadsInCurrentGcEpoch.erase(ctx);
  }
}

void RAIDController::PrintSegmentPos() {
  for (auto segment :mOpenSegments) {
    debug_error("segment id %u pos %u status %u\n", segment->GetSegmentId(), 
        segment->GetPos(), (int)segment->GetStatus());
  }

}

void RAIDController::Dump()
{
  // Dump address map
//  for (uint32_t i = 0; i < Configuration::GetStorageSpaceInBytes() / Configuration::GetBlockSize(); ++i) {
//    if (mAddressMap[i].segment != nullptr) {
//      printf("%llu %u %u %u\n", 
//          i * Configuration::GetBlockSize() * 1ull, 
//          mAddressMap[i].segment->GetSegmentId(),
//          mAddressMap[i].zoneId,
//          mAddressMap[i].offset);
//    }
//  }

  std::map<uint64_t, Segment*> orderedSegments;
  // Dump the information of each segment
  for (auto segment : mOpenSegments) {
    orderedSegments[segment->GetSegmentId()] = segment;
  }
  for (auto segment : mSegmentsToSeal) {
    orderedSegments[segment->GetSegmentId()] = segment;
  }
  for (auto segment : mSealedSegments) {
    orderedSegments[segment->GetSegmentId()] = segment;
  }
  for (auto pr : orderedSegments) {
    pr.second->Dump();
  }

  std::map<uint32_t, uint32_t> appendLatencies;
  std::map<uint32_t, uint32_t> parityLatencies;

  for (auto pr : orderedSegments) {
    auto mp = pr.second->RetrieveAppendLatencies();
    for (auto it : mp) {
      appendLatencies[it.first] += it.second;
    }
    mp = pr.second->RetrieveParityLatencies();
    for (auto it : mp) {
      parityLatencies[it.first] += it.second;
    }
  }

  if (Configuration::GetEnableIOLatencyTest()) {
    DumpLatencies(appendLatencies, "append");
    DumpLatencies(parityLatencies, "parity");
    DumpLatencies(mWriteLatCnt, "write");
    DumpLatencies(mReadLatCnt, "read");
  }

  printf("RSS: %lu KiB\n", getRss());
  StatsRecorder::DestroyInstance();
}
