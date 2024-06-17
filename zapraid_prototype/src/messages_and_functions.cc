#include "messages_and_functions.h"
#include <vector>
#include <queue>
#include "common.h"
#include "device.h"
#include "segment.h"
#include "spdk/thread.h"
#include "raid_controller.h"
#include <sys/time.h>

#include <sched.h>

void handleContext(RequestContext *context);

// index thread
void updatePba2(void *arg1, void *arg2)
{
  struct timeval s, e;
  static int cnt = 0, stuckPrint = 0;
  gettimeofday(&s, 0);
  cnt++;
  RAIDController *ctrl = reinterpret_cast<RAIDController*>(arg1);
  RequestContext *usrCtx = reinterpret_cast<RequestContext*>(arg2);
  uint32_t blockSize = Configuration::GetBlockSize();
  uint32_t numBlocks = usrCtx->size / Configuration::GetBlockSize();
  bool success = true;

  // reuse curOffset for index updating
  for (uint32_t i = usrCtx->curOffset; i < numBlocks; ++i) {
    uint64_t lba = usrCtx->lba + i * blockSize;
    IndexMapStatus s = ctrl->UpdateIndex(lba, usrCtx->pbaArray[i]);
    if (s != kHit) {
      // prepare to evict or read
      if (s == kNeedEvict || s == kNeedRead) {
        RequestContext* indCtx = ctrl->PrepareIndexContext(s, lba);
        // use the associated requests to save one thread message
        // blockSize here is useless
        if (indCtx != nullptr) {
          indCtx->associatedRequests.push_back(std::make_pair(usrCtx,
                std::make_pair(i, blockSize)));
          ctrl->ExecuteIndex(indCtx);   // message to dispatch thread
        } else {
          // waiting for evictions, store usrCtx first 
          ctrl->EnqueueWaitIndex(usrCtx, lba);
        }
      } else if (s == kFetching) {
        // no need to send message to dispatch thread; manage in index
        // thread to avoid enqueuing context after request is handled
        ctrl->EnqueueWaitIndex(usrCtx, lba);  
      } else {
        debug_error("s = %d\n", s);
        assert(0);
      }
      success = false;
      break;
    } else {
      ctrl->RemoveWaitIndex(usrCtx, lba);
      usrCtx->curOffset++;
    }
  }

  debug_warn("updatePba2 usrCtx->lba %lu cnt %u\n", usrCtx->lba, cnt);
  // call back to user
  if (success) {
    usrCtx->status = WRITE_COMPLETE;
    // debug
    ctrl->CompleteWrite();

    if (usrCtx->cb_fn != nullptr) {
      usrCtx->cb_fn(usrCtx->cb_args);
    }
    usrCtx->available = true;
  }
  gettimeofday(&e, 0);
  ctrl->MarkIndexThreadUpdatePbaTime(s, e);
}

void updatePba(void *args) {
  UpdatePbaArgs *qArgs = reinterpret_cast<UpdatePbaArgs*>(args);
  RAIDController *ctrl = qArgs->ctrl;
  RequestContext *ctx = qArgs->ctx;
  free(qArgs);

  assert(ctx->status == WRITE_INDEX_UPDATING);
  updatePba2(ctrl, ctx);
}

// index thread
void handleIndexContextMsg(void *args) {
  RequestContext* ctx = reinterpret_cast<RequestContext*>(args);
  if (ctx->available) {
    assert(0);
  }
  if (ctx->ctrl == nullptr) { 
    debug_error("ctx->ctrl == nullptr, status %d, tyep %d, lba %lu, size %u\n",
        ctx->status, ctx->type, ctx->lba, ctx->size);
    assert(0);
  }
  handleIndexContextMsg2(args, nullptr);
}

void handleIndexContextMsg2(void *arg1, void *arg2) {
  RequestContext* ctx = reinterpret_cast<RequestContext*>(arg1);
  if (ctx->ctrl == nullptr) { 
    debug_e("ctx->ctrl == nullptr\n");
    assert(0);
  }
  ctx->ctrl->HandleIndexContext(ctx);
}

static void dummy_disconnect_handler(struct spdk_nvme_qpair *qpair, void *poll_group_ctx)
{
}

int handleIoCompletions(void *args)
{
  static double mTime = 0;
  static int timeCnt = 0;
  {
    double timeNow = GetTimestampInUs();
    if (mTime == 0) {
      printf("[DEBUG] %s %d\n", __func__, timeCnt);
      mTime = timeNow;
    } else {
      if (timeNow - mTime > 10.0) {
        mTime += 10.0;
        timeCnt += 10;
        printf("[DEBUG] %s %d\n", __func__, timeCnt);
      }
    }
  }

  struct spdk_nvme_poll_group* pollGroup = (struct spdk_nvme_poll_group*)args;
  int r = 0;
  r = spdk_nvme_poll_group_process_completions(pollGroup, 0, dummy_disconnect_handler);
  return r > 0 ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

bool checkPendingWrites(RAIDController* ctrl) {
  static timeval s, e;
  static int starve_time = 5;
  bool busy = false;
  gettimeofday(&s, 0);
  std::map<Zone*, std::map<uint32_t, RequestContext*>>& zoneWriteQ =
    ctrl->GetPendingZoneWriteQueue();
  for (auto& it : zoneWriteQ) {
    Zone* zone = it.first;
    uint64_t pos = zone->GetPos();
    if (it.second.count(pos) == 0) {
      continue;
    }
    realZoneWrite(it.second[pos]);
    it.second.erase(pos);
    busy = true;

    if (it.second.empty()) {
      zoneWriteQ.erase(zone);
    }
    break;
  }

  gettimeofday(&e, 0);
  ctrl->MarkIoThreadCheckPendingWritesTime(s, e);
  return busy;
}

int handleIoPendingZoneWrites(void *args) {
  RAIDController *ctrl = (RAIDController*)args;
  int r = 0;
  bool busy = checkPendingWrites(ctrl); 

  return busy ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

int ioWorker(void *args)
{
  IoThread *ioThread = (IoThread*)args;
  struct spdk_thread *thread = ioThread->thread;
  RAIDController* ctrl = ioThread->controller;
  spdk_set_thread(thread);
  spdk_poller_register(handleIoCompletions, ioThread->group, 0);
  spdk_poller_register(handleIoPendingZoneWrites, ctrl, 0);
  while (true) {
    spdk_thread_poll(thread, 0, 0);
  }
}

static bool contextReady(RequestContext *ctx)
{
  if (ctx->successBytes > ctx->targetBytes) {
    debug_error("ctx->successBytes %u ctx->targetBytes %u\n", 
        ctx->successBytes, ctx->targetBytes);
    printf("line %d ctx->successBytes %u ctx->targetBytes %u\n", 
        __LINE__, ctx->successBytes, ctx->targetBytes); 
    assert(0);
  }
  return ctx->successBytes == ctx->targetBytes;
}

// the handling of write request finishes at updating PBA;
// the handling of read request finishes here (reaping) 
static void handleUserContext(RequestContext *usrCtx)
{
  debug_warn("usrCtx %p\n", usrCtx);
  ContextStatus &status = usrCtx->status;
  RAIDController *ctrl = usrCtx->ctrl;
  assert(contextReady(usrCtx));
  if (status == WRITE_REAPING) {
    status = WRITE_INDEX_UPDATING;
    // reuse for index updating, avoid repeated PBA update 
    // repeated PBA updates may cause repeated block invalidation.
    usrCtx->curOffset = 0; 
    if (!Configuration::GetEventFrameworkEnabled()) {
      UpdatePbaArgs *args = (UpdatePbaArgs*)calloc(1, sizeof(UpdatePbaArgs));
      args->ctrl = ctrl;
      args->ctx = usrCtx;
      thread_send_msg(ctrl->GetIndexThread(), updatePba, args);
    } else {
      event_call(Configuration::GetIndexThreadCoreId(),
                 updatePba2, ctrl, usrCtx);
    }
  } else if (status == READ_REAPING) {
    ctrl->RemoveRequestFromGcEpochIfNecessary(usrCtx);
    status = READ_COMPLETE;
    ctrl->CompleteRead();

    if (usrCtx->cb_fn != nullptr) {
      usrCtx->cb_fn(usrCtx->cb_args);
    }
    usrCtx->available = true;
  }
}

void handleGcContext(RequestContext *context)
{
  ContextStatus &status = context->status;
  if (status == WRITE_REAPING) {
    status = WRITE_COMPLETE;
  } else if (status == READ_REAPING) {
    status = READ_COMPLETE;
  } else {
    assert(0);
  }
  context->available = true;
}

void handleIndexContext(RequestContext *context)
{
  if (context->ctrl == nullptr) {
    debug_e("controller is empty!\n");
    assert(0);
  }
  ContextStatus &status = context->status;
  if (status == WRITE_INDEX_REAPING || status == READ_INDEX_REAPING) {
    if (!Configuration::GetEventFrameworkEnabled()) {
      thread_send_msg(context->ctrl->GetIndexThread(), handleIndexContextMsg,
          context);
    } else {
      event_call(Configuration::GetIndexThreadCoreId(),
                 handleIndexContextMsg2, context, nullptr);
    }
  } else {
    debug_error("status %d\n", status);
    assert(0);
  }
  // should not set available to true!
}

static void handleStripeUnitContext(RequestContext *context)
{
  static double part1 = 0, part2 = 0, part3 = 0;
  static int count = 0;
  struct timeval s, e;
  uint32_t blockSize = Configuration::GetBlockSize();
  RAIDController* ctrl = context->ctrl;

  ContextStatus &status = context->status;
  switch (status) {
    case WRITE_REAPING:
      debug_warn("context->successBytes %u context->targetBytes %u stripe %p\n",
          context->successBytes, context->targetBytes, context->associatedStripe);
      if (context->successBytes > context->targetBytes) {
        debug_error("context->successBytes %u context->targetBytes %u\n",
            context->successBytes, context->targetBytes);
        assert(0);
      }

      if (context->successBytes == context->targetBytes) {
        StripeWriteContext *stripe = context->associatedStripe;
        debug_warn("stripe %p\n", stripe);
        if (stripe != nullptr) {
          Segment* seg = nullptr;
          stripe->successBytes += context->ioContext.size * blockSize;
          if (Configuration::GetEnableIOLatencyTest() && 
              context->successBytes == Configuration::GetStripeUnitSize(0)) {
            ctrl->MarkWriteLatency(context);
          }

          if (stripe->successBytes > stripe->targetBytes +
              stripe->metaTargetBytes) {
            debug_error("stripe->successBytes %u stripe->targetBytes %u"
                " stripe->metaTargetBytes %u\n",
                stripe->successBytes, stripe->targetBytes, 
                stripe->metaTargetBytes);
            assert(0);
          }

          debug_warn("stripe->successBytes %u stripe->targetBytes %u"
              " stripe->metaTargetBytes %u\n",
              stripe->successBytes, stripe->targetBytes,
              stripe->metaTargetBytes);
          if (stripe->successBytes != stripe->targetBytes + 
              stripe->metaTargetBytes) {
            break;
          }

          // Acknowledge the completion only after the whole or partial stripe
          // persists
          // update pba array through handleContext(parent)
          for (auto slot : stripe->ioContext) {
            if (slot->available) continue;
            if (slot->successBytes < slot->targetBytes) {
              // not used because RAIZN allows the following units to wait 
              continue;  
            }
            uint32_t totalContextSize = slot->ioOffset * blockSize;
            for (auto reqSizeIt_ : slot->associatedRequests) {
              RequestContext *parent = reqSizeIt_.first;
              uint64_t uCtxOffset = reqSizeIt_.second.first;
              uint32_t uCtxSize = reqSizeIt_.second.second;

              if (parent) {
                for (int dlba = 0; dlba < uCtxSize; dlba += blockSize) {
                  auto addr = slot->GetPba();
                  addr.offset += (totalContextSize + dlba) / blockSize;
                  debug_warn("parent lba %lu offset %lu dlba %d size %u\n", 
                      parent->lba, uCtxOffset, dlba, uCtxSize);
                  parent->pbaArray[uCtxOffset + dlba / blockSize] = addr;
                }

                debug_warn("parent %p parent->successBytes %u parent->targetBytes %u\n",
                    parent,
                    parent->successBytes, parent->targetBytes);
                parent->successBytes += uCtxSize; //slot->targetBytes;
                if (parent->successBytes > parent->targetBytes) {
                  debug_error("parent->successBytes %u parent->targetBytes %u\n",
                      parent->successBytes, parent->targetBytes);
                  assert(0);
                }
                assert(parent->successBytes <= parent->targetBytes);
                if (contextReady(parent)) {
                  debug_warn("handle parent %p\n", parent);
                  handleContext(parent);
                }
              }
              totalContextSize += reqSizeIt_.second.second;
            }
            // don't use "=="
            // Some requests may not have parents, e.g., header and footer
            assert(totalContextSize <= slot->targetBytes);

            slot->segment->WriteComplete(slot);
            status = WRITE_COMPLETE;

            // cannot reclaim the slot if only partial stripe is written
              slot->available = true;

            if (slot->segment != nullptr) {
              if (seg == nullptr) {
                seg = slot->segment;
              } else {
                assert(seg == slot->segment);
              }
            }
          }

          if (Configuration::GetSystemMode() == RAIZN_SIMPLE) {
            seg->RaiznWriteComplete();
          }
        }
      } else {
        debug_warn("context->successBytes %u context->targetBytes %u\n", context->successBytes, context->targetBytes); 
      }
      break;
    case READ_REAPING:
    case DEGRADED_READ_REAPING:
      if (context->needDegradedRead) {
        context->needDegradedRead = false;
        status = DEGRADED_READ_REAPING;
        context->segment->ReadStripe(context);
      } else if (context->successBytes >= context->targetBytes) {
        if (context->successBytes > context->targetBytes) {
          printf("context->successBytes %u context->targetBytes %u\n",
              context->successBytes, context->targetBytes);
          assert(0);
        }
        if (Configuration::GetEnableIOLatencyTest()) {
          ctrl->MarkReadLatency(context);
        }
        context->segment->ReadComplete(context);
        for (auto& it : context->associatedRequests) {
          auto& parent = it.first;
          parent->successBytes += it.second.second; 
          assert(parent->successBytes <= parent->targetBytes);
          if (contextReady(parent)) {
            debug_warn("handle %p\n", parent);
            handleContext(parent);
          }
        }
        status = READ_COMPLETE;
        context->available = true;
        context->segment->ReclaimReadContext(context->associatedRead);
      }
      break;
    case DEGRADED_READ_SUB:
      assert(context->associatedRequests.size() > 0);

      for (auto& it : context->associatedRequests) {
        auto& parent = it.first;
        parent->successBytes += it.second.second;
        if (contextReady(parent)) {
          handleStripeUnitContext(parent);
        }
      }

      context->segment->ReadComplete(context);
      break;
    case DEGRADED_READ_META:
      if (contextReady(context)) {
        status = DEGRADED_READ_REAPING;
        context->segment->ReadStripe(context);
      }
      break;
    case RESET_REAPING:
      status = RESET_COMPLETE;
      context->available = true;
      break;
    case FINISH_REAPING:
      status = FINISH_COMPLETE;
      context->available = true;
      break;
    default:
      printf("Error in context handling! %p\n", context);
      assert(0);
  }
}

void handleContext(RequestContext *context)
{
  ContextType type = context->type;
  if (type == USER) {
    handleUserContext(context);
  } else if (type == STRIPE_UNIT) {
    handleStripeUnitContext(context);
  } else if (type == GC) {
    handleGcContext(context);
  } else if (type == INDEX) {
    handleIndexContext(context);
  }
}

void handleEventCompletion2(void *arg1, void *arg2)
{
  RequestContext *slot = (RequestContext*)arg1;
  struct timeval s, e;
  gettimeofday(&s, 0);
  handleContext(slot);
  gettimeofday(&e, 0);
  slot->ctrl->MarkDispatchThreadHandleContextTime(s, e);
}

void handleEventCompletion(void *args)
{
  handleEventCompletion2(args, nullptr);
}

int handleEventsDispatch(void *args)
{
  // debug
  static double mTime = 0;
  static int timeCnt = 0;
  bool printFlag = false;
  double timeNow = GetTimestampInUs();
  {
    if (mTime == 0) {
      printf("[DEBUG] %s %d\n", __func__, timeCnt);
      mTime = timeNow;
      printFlag = true;
    } else {
      if (timeNow - mTime > 10.0) {
        mTime += 10.0;
        timeCnt += 10;
        printf("[DEBUG] %s %d\n", __func__, timeCnt);
        printFlag = true;
      }
    }
  }
  static timeval s, e;

  bool busy = false;
  RAIDController *ctrl = (RAIDController*)args;
  gettimeofday(&s, 0);

  std::queue<RequestContext*>& writeQ = ctrl->GetWriteQueue();
  if (Configuration::StuckDebugMode()) {
    Configuration::IncreaseStuckCounter();
//    if (Configuration::GetStuckCounter() > 2) 
    if (Configuration::StuckDebugModeFinished(timeNow)) 
    {
      debug_e("Stucked. Exit\n");
      assert(0);
    }
  }
  while (!writeQ.empty()) {
    RequestContext *ctx = writeQ.front();
    ctrl->WriteInDispatchThread(ctx);

    if (ctx->curOffset == ctx->size / Configuration::GetBlockSize()) {
      busy = true;
      writeQ.pop();
    } else {
      break;
    }
  }

  std::queue<RequestContext*>& readPrepareQ = ctrl->GetReadPrepareQueue();
  while (!readPrepareQ.empty()) {
    RequestContext *ctx = readPrepareQ.front();
    ctrl->ReadInDispatchThread(ctx);
    readPrepareQ.pop();
    busy = true;
  }

  std::queue<RequestContext*>& readReapingQ = ctrl->GetReadReapingQueue();
  while (!readReapingQ.empty()) {
    RequestContext *ctx = readReapingQ.front();
    ctrl->ReadInDispatchThread(ctx);

    if (ctx->curOffset == ctx->size / Configuration::GetBlockSize()) {
      busy = true;
      readReapingQ.pop();
    } else {
      break;
    }
  }

  // new implementation: write index
  std::queue<RequestContext*>& writeIndexQ = ctrl->GetWriteIndexQueue();
  while (!writeIndexQ.empty()) {
    RequestContext *ctx = writeIndexQ.front();
//    ctrl->WriteIndexInDispatchThread(ctx);
    ctrl->WriteInDispatchThread(ctx);

    if (ctx->curOffset == ctx->size / Configuration::GetBlockSize()) {
      busy = true;
      writeIndexQ.pop();
    } else {
      break;
    }
  }

  // new implementation: read index
  std::queue<RequestContext*>& readIndexQ = ctrl->GetReadIndexQueue();
  while (!readIndexQ.empty()) {
    RequestContext *ctx = readIndexQ.front();
    ctrl->ReadInDispatchThread(ctx);

    if (ctx->curOffset == ctx->size / Configuration::GetBlockSize()) {
      busy = true;
      readIndexQ.pop();
    } else {
      break;
    }
  }

  gettimeofday(&e, 0);
  ctrl->MarkDispatchThreadEventTime(s, e);

  return busy ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

int handleBackgroundTasks(void *args) {
  // debug
  static double mTime = 0;
  static int timeCnt = 0;
  static bool stuckFlag = 0;
  static uint64_t lastIOBytes = 0;
  static uint64_t savedTimeCnt = 0;
  struct timeval s, e;
  gettimeofday(&s, 0);

  RAIDController *raidController = (RAIDController*)args;
  {
    double timeNow = GetTimestampInUs();
    uint64_t sw, sr, cw, cr;
    uint64_t isw, isr, icw, icr;
    if (mTime == 0) {
      raidController->RetrieveWRCounts(sw, sr, cw, cr);
      printf("[DEBUG] %s %d (%lu %lu %lu %lu)\n", __func__, timeCnt,
          sw, sr, cw, cr);
      mTime = timeNow;
    } else {
      if (timeNow - mTime > 10.0) {
        raidController->RetrieveWRCounts(sw, sr, cw, cr);
        raidController->RetrieveIndexWRCounts(isw, isr, icw, icr);
        mTime += 10.0;
        timeCnt += 10;
        uint64_t IOBytes, writeBytes;
        IOBytes = StatsRecorder::getInstance()->getTotalIOBytes();
        writeBytes = StatsRecorder::getInstance()->getTotalWriteBytes();
        printf("[DEBUG] %s %d (%lu %lu %lu %lu) index (%lu %lu %lu %lu) "
            "read/write %.3lf/%.3lf GiB\n", 
            __func__, timeCnt, sw, sr, cw, cr,
            isw, isr, icw, icr,
            (double)(IOBytes - writeBytes) / 1024 / 1024 / 1024,
            (double)(writeBytes) / 1024 / 1024 / 1024);
        raidController->printCompletionThreadTime();
        raidController->printIoThreadTime(timeCnt * 1000000.0);
        raidController->printDispatchThreadTime(timeCnt * 1000000.0);
        raidController->printIndexThreadTime(timeCnt * 1000000.0);
        if (timeCnt > 20 && timeCnt > savedTimeCnt + 5 && 
            lastIOBytes == IOBytes && cw + cr < sw + sr) {
          // I/O not processing, but some requests needs to be processed 
          printf("[DEBUG] totally stuck\n");
          Configuration::SetStuckDebugMode(timeNow);
          assert(0);
        } else {
        }

        if (timeCnt > savedTimeCnt + 5) { 
          lastIOBytes = StatsRecorder::getInstance()->getTotalIOBytes();
          savedTimeCnt = timeCnt;
        }
        if (timeCnt % 30 == 0) {
          StatsRecorder::getInstance()->Dump();
        }
//        raidController->PrintSegmentPos();
      }
    }
  }

  bool hasProgress = false;
  hasProgress |= raidController->ProceedGc();
  hasProgress |= raidController->CheckSegments(); // including meta zones
  gettimeofday(&e, 0);
  raidController->MarkDispatchThreadBackgroundTime(s, e);

  return hasProgress ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

int handleIndexTasks(void *args) {
  RAIDController *raidController = (RAIDController*)args;
  double timeNow = GetTimestampInUs();
  static double mTime = 0;
  static int timeCnt = 0;
  {
    if (mTime == 0) {
      printf("[DEBUG] %s %d\n", __func__, timeCnt);
      mTime = timeNow;
    } else {
      if (timeNow - mTime > 10.0) {
        mTime += 10.0;
        timeCnt += 10;
        printf("[DEBUG] %s %d size %lu\n", __func__, timeCnt,
            raidController->GetWaitIndexSize());
//        raidController->PrintWaitIndexTime(timeNow);
      }
    }
  }

  uint64_t numTasks = 0; 
  // need this because for sequential workloads, reading a mapping block
  // needs to resume more than 4 user requests
  numTasks = raidController->rescheduleIndexTask();
  return //SPDK_POLLER_IDLE; 
    (numTasks > 0) ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

int dispatchWorker(void *args)
{
  RAIDController *raidController = (RAIDController*)args;
  struct spdk_thread *thread = raidController->GetDispatchThread();
  spdk_set_thread(thread);
  spdk_poller *p1, *p2; 
  p1 = spdk_poller_register(handleEventsDispatch, raidController, 1);
  p2 = spdk_poller_register(handleBackgroundTasks, raidController, 1);
  raidController->SetEventPoller(p1);
  raidController->SetBackgroundPoller(p2);
  while (true) {
    spdk_thread_poll(thread, 0, 0);
  }
}

int ecWorker(void *args)
{
  RAIDController *raidController = (RAIDController*)args;
  struct spdk_thread *thread = raidController->GetEcThread();
  spdk_set_thread(thread);
  while (true) {
    spdk_thread_poll(thread, 0, 0);
  }
}

int indexWorker(void *args) {
  RAIDController *raidController = (RAIDController*)args;
  struct spdk_thread *thread = raidController->GetIndexThread();
  spdk_set_thread(thread);
  spdk_poller *p1, *p2; 
  p1 = spdk_poller_register(handleIndexTasks, raidController, 1);
  while (true) {
    spdk_thread_poll(thread, 0, 0);
  }
}

int completionWorker(void *args) {
  RAIDController *raidController = (RAIDController*)args;
  struct spdk_thread *thread = raidController->GetCompletionThread();
  spdk_set_thread(thread);
  while (true) {
    spdk_thread_poll(thread, 0, 0);
  }
}

void executeRequest(void *arg1, void *arg2)
{
  Request *req = reinterpret_cast<Request*>(arg1);
  req->controller->Execute(
    req->offset, req->size, req->data,
    req->type == 'W', req->cb_fn, req->cb_args);
  free(req);
}

void registerIoCompletionRoutine(void *arg1, void *arg2)
{
  IoThread *ioThread = (IoThread*)arg1;
  spdk_poller_register(handleIoCompletions, ioThread->group, 0);
  spdk_poller_register(handleIoPendingZoneWrites, ioThread->controller, 0);
}

void registerDispatchRoutine(void *arg1, void *arg2)
{
  RAIDController *raidController = reinterpret_cast<RAIDController*>(arg1);
  spdk_poller *p1, *p2;
  p1 = spdk_poller_register(handleEventsDispatch, raidController, 1);
  p2 = spdk_poller_register(handleBackgroundTasks, raidController, 1);
  raidController->SetEventPoller(p1);
  raidController->SetBackgroundPoller(p2);
}

void enqueueRequest2(void *arg1, void *arg2)
{
  struct timeval s, e;
  gettimeofday(&s, 0);
  RequestContext *ctx = reinterpret_cast<RequestContext*>(arg1);
  if (ctx->req_type == 'W') {
    ctx->ctrl->EnqueueWrite(ctx);
  } else {
    if (ctx->status == READ_PREPARE) {
      ctx->ctrl->EnqueueReadPrepare(ctx);
    } else if (ctx->status == READ_REAPING) {
      ctx->ctrl->EnqueueReadReaping(ctx);
    }
  }
  gettimeofday(&e, 0);
  ctx->ctrl->MarkDispatchThreadEnqueueTime(s, e);
}

void enqueueRequest(void *args)
{
  enqueueRequest2(args, nullptr);
}

void enqueueIndexRequest2(void *arg1, void *arg2)
{
  RequestContext *ctx = reinterpret_cast<RequestContext*>(arg1);
  if (ctx->req_type == 'W') {
    assert(ctx->status == WRITE_INDEX_REAPING);
    ctx->ctrl->EnqueueWriteIndex(ctx);
  } else {
    assert(ctx->status == READ_INDEX_REAPING);
    ctx->ctrl->EnqueueReadIndex(ctx);
  }
}

void enqueueIndexRequest(void *args)
{
  enqueueIndexRequest2(args, nullptr);
}

void queryPba2(void *arg1, void *arg2)
{
  RAIDController *ctrl = reinterpret_cast<RAIDController*>(arg1);
  RequestContext *ctx = reinterpret_cast<RequestContext*>(arg2);
  struct timeval s, e;
  gettimeofday(&s, 0);
  bool success = true;
  uint32_t blockSize = Configuration::GetBlockSize();
  for (uint32_t i = 0; i < ctx->size / blockSize; ++i) {
    PhysicalAddr phyAddr;
    uint64_t lba = ctx->lba + i * blockSize;
    IndexMapStatus s = ctrl->LookupIndex(lba, &phyAddr);
    if (s != kHit) {
      // prepare to evict or read
      if (s == kNeedEvict || s == kNeedRead) {
        RequestContext* indCtx = ctrl->PrepareIndexContext(s, lba);
        // use the associated requests to save one thread message
        // blockSize here is useless
        if (indCtx != nullptr) {
          indCtx->associatedRequests.push_back(std::make_pair(ctx,
                std::make_pair(i, blockSize)));
          ctrl->ExecuteIndex(indCtx);   // message to dispatch thread
        } else {
          // waiting for evictions, store ctx first 
          ctrl->EnqueueWaitIndex(ctx, lba);
        }
      } else if (s == kFetching) {
        // no need to send message to dispatch thread; manage in index
        // thread to avoid enqueuing context after request is handled
        ctrl->EnqueueWaitIndex(ctx, lba);
      } else {
        debug_error("s = %d\n", s);
        assert(0);
      }
      success = false;
      break;
    } else {
      // avoid repeated context
      ctrl->RemoveWaitIndex(ctx, lba);
      ctx->curOffset++;
      // replace the lba with pba
      if (phyAddr.segment != nullptr) {
        ctx->pbaArray[i] = phyAddr;
        if (phyAddr.segment->isInvalid()) {
          debug_error("invalid segment for id %u for lba %lu\n",
              phyAddr.segment->GetSegmentId(), 
              lba);
          assert(0);
        }
      } else {
        ctx->pbaArray[i].segment = nullptr;
      }
    }
  }

  if (success) {
    // only change the status if the whole physical array is ready
    ctx->status = READ_REAPING;
    ctx->curOffset = 0;

    if (!Configuration::GetEventFrameworkEnabled()) {
      thread_send_msg(ctrl->GetDispatchThread(), enqueueRequest, ctx);
    } else {
      event_call(Configuration::GetDispatchThreadCoreId(),
          enqueueRequest2, ctx, nullptr);
    }
  }
  gettimeofday(&e, 0);
  ctrl->MarkIndexThreadQueryPbaTime(s, e);
}

void queryPba(void *args)
{
  QueryPbaArgs *qArgs = (QueryPbaArgs*)args;
  RAIDController *ctrl = qArgs->ctrl;
  RequestContext *ctx = qArgs->ctx;
  free(qArgs);

  queryPba2(ctrl, ctx);
}

void zoneWrite2(void *arg1, void *arg2)
{
  static timeval s, e;
  RequestContext *slot = reinterpret_cast<RequestContext*>(arg1);
  auto ioCtx = slot->ioContext;

  gettimeofday(&s, 0);
  slot->stime = timestamp();

  if (Configuration::GetEnableIOLatencyTest()) {
    slot->timeA = s;
  }

  // only one io thread can do this
  if (slot->zone->GetPos() != slot->offset) {
    slot->ctrl->EnqueueZoneWrite(slot);
  } else { 
    realZoneWrite(slot);
  }

  gettimeofday(&e, 0);
  if (slot->ctrl == nullptr) {
    debug_warn("slot nullptr %p\n", slot);
    assert(0); 
  }
  slot->ctrl->MarkIoThreadZoneWriteTime(s, e);
}

void bufferCopy2(void *arg1, void *arg2) {
  bufferCopy(arg1);
}

void bufferCopy(void *args) {
  static timeval s, e;
  static bool first = true;

  BufferCopyArgs* bcArgs = reinterpret_cast<BufferCopyArgs*>(args);
  RequestContext* slot = bcArgs->slot;
  Device* device = bcArgs->dev;
  RAIDController* ctrl = bcArgs->slot->ctrl;
  gettimeofday(&s, 0);
  if (!first) {
    ctrl->MarkCompletionThreadIdleTime(e, s);
  }
  first = false;

  auto ioCtx = slot->ioContext;

  {
    uint32_t blockSize = Configuration::GetBlockSize();
    uint32_t ptr = 0;
    for (auto& it : slot->associatedRequests) {
      auto& parent = it.first;
      uint32_t uCtxOffset = it.second.first;
      uint32_t uCtxSize = it.second.second;

      if (parent) {
        memcpy((uint8_t*)ioCtx.data + ptr, 
            parent->data + uCtxOffset * blockSize, uCtxSize);
        ptr += uCtxSize;
      } else {
        memset((uint8_t*)ioCtx.data + ptr, 0, uCtxSize);
      }
    }
  }

  delete bcArgs;

  if (Configuration::GetEventFrameworkEnabled()) {
    if (slot->append) {
      device->issueIo2(zoneAppend2, slot);
    } else {
      device->issueIo2(zoneWrite2, slot);
    }
  } else {
    if (slot->append) {
      device->issueIo(zoneAppend, slot);
    } else {
      device->issueIo(zoneWrite, slot);
    }
  }
  gettimeofday(&e, 0);
  ctrl->MarkCompletionThreadBufferCopyTime(s, e);
}

void zoneWrite(void *args)
{
  zoneWrite2(args, nullptr);
}

void zoneRead2(void *arg1, void *arg2)
{
  RequestContext *slot = reinterpret_cast<RequestContext*>(arg1);
  auto ioCtx = slot->ioContext;
  slot->stime = timestamp();

  int rc = 0;
  if (Configuration::GetEnableIOLatencyTest()) {
    gettimeofday(&slot->timeA, 0);
  }

  if (Configuration::GetDeviceSupportMetadata()) {
    rc = spdk_nvme_ns_cmd_read_with_md(ioCtx.ns, ioCtx.qpair,
                                  ioCtx.data, ioCtx.metadata,
                                  ioCtx.offset, ioCtx.size,
                                  ioCtx.cb, ioCtx.ctx,
                                  ioCtx.flags, 0, 0);
  } else {
    rc = spdk_nvme_ns_cmd_read(ioCtx.ns, ioCtx.qpair,
                                  ioCtx.data, ioCtx.offset, ioCtx.size,
                                  ioCtx.cb, ioCtx.ctx,
                                  ioCtx.flags);
  }
  if (rc != 0) {
    fprintf(stderr, "Device read error!\n");
  }
  assert(rc == 0);
}

void realZoneWrite(RequestContext *slot)
{
  auto ioCtx = slot->ioContext;
  int rc = 0;
  if (Configuration::GetEnableIOLatencyTest()) {
    gettimeofday(&slot->timeA, 0);
  }

  if (Configuration::GetDeviceSupportMetadata()) {
    rc = spdk_nvme_ns_cmd_write_with_md(ioCtx.ns, ioCtx.qpair,
                                  ioCtx.data, ioCtx.metadata,
                                  ioCtx.offset, ioCtx.size,
                                  ioCtx.cb, ioCtx.ctx,
                                  ioCtx.flags, 0, 0);
  } else {
    rc = spdk_nvme_ns_cmd_write(ioCtx.ns, ioCtx.qpair,
                                  ioCtx.data, ioCtx.offset, ioCtx.size,
                                  ioCtx.cb, ioCtx.ctx,
                                  ioCtx.flags);
  }
  if (rc != 0) {
    fprintf(stderr, "Device write error!\n");
  }
  assert(rc == 0);
}

void zoneRead(void *args)
{
  zoneRead2(args, nullptr);
}

void zoneAppend2(void *arg1, void *arg2)
{
  RequestContext *slot = reinterpret_cast<RequestContext*>(arg1);
  auto ioCtx = slot->ioContext;
  slot->stime = timestamp();

//  fprintf(stderr, "Append %lx %lu\n", ioCtx.offset, ioCtx.size);

  int rc = 0;
  if (Configuration::GetEnableIOLatencyTest()) {
    gettimeofday(&slot->timeA, 0);
  }
  
//  {
//    uint32_t blockSize = Configuration::GetBlockSize();
//    uint32_t ptr = 0;
//    for (auto& it : slot->associatedRequests) {
//      auto& parent = it.first;
//      uint32_t uCtxOffset = it.second.first;
//      uint32_t uCtxSize = it.second.second;
//
//      if (parent) {
//        memcpy((uint8_t*)ioCtx.data + ptr, 
//            parent->data + uCtxOffset * blockSize, uCtxSize);
//        ptr += uCtxSize;
//      } else {
//        memset((uint8_t*)ioCtx.data + ptr, 0, uCtxSize);
//      }
//    }
//  }

  if (Configuration::GetDeviceSupportMetadata()) {
    rc = spdk_nvme_zns_zone_append_with_md(ioCtx.ns, ioCtx.qpair,
                                  ioCtx.data, ioCtx.metadata,
                                  ioCtx.offset, ioCtx.size,
                                  ioCtx.cb, ioCtx.ctx,
                                  ioCtx.flags, 0, 0);
  } else {
    rc = spdk_nvme_zns_zone_append(ioCtx.ns, ioCtx.qpair,
                                      ioCtx.data, ioCtx.offset, ioCtx.size,
                                      ioCtx.cb, ioCtx.ctx,
                                      ioCtx.flags);
  }
  if (rc != 0) {
    fprintf(stderr, "Device append error!\n");
  }
  assert(rc == 0);
}

void zoneAppend(void *args)
{
  zoneAppend2(args, nullptr);
}

void zoneReset2(void *arg1, void *arg2)
{
  RequestContext *slot = reinterpret_cast<RequestContext*>(arg1);
  auto ioCtx = slot->ioContext;
  slot->stime = timestamp();
  int rc = spdk_nvme_zns_reset_zone(ioCtx.ns, ioCtx.qpair, ioCtx.offset, 0, ioCtx.cb, ioCtx.ctx);
  if (rc != 0) {
    fprintf(stderr, "Device reset error!\n");
  }
  assert(rc == 0);
}

void zoneReset(void *args)
{
  zoneReset2(args, nullptr);
}

void zoneFinish2(void *arg1, void *arg2)
{
  RequestContext *slot = reinterpret_cast<RequestContext*>(arg1);
  auto ioCtx = slot->ioContext;
  slot->stime = timestamp();

  int rc = spdk_nvme_zns_finish_zone(ioCtx.ns, ioCtx.qpair, ioCtx.offset, 0, ioCtx.cb, ioCtx.ctx);
  if (rc != 0) {
    fprintf(stderr, "Device close error!\n");
  }
  assert(rc == 0);
}

void zoneFinish(void *args)
{
  zoneFinish2(args, nullptr);
}

void tryDrainController(void *args)
{
  DrainArgs *drainArgs = (DrainArgs *)args;
  drainArgs->ctrl->CheckSegments();
  drainArgs->ctrl->ReclaimContexts();
  drainArgs->ctrl->ProceedGc();
  drainArgs->success = drainArgs->ctrl->GetNumInflightRequests() == 0 && !drainArgs->ctrl->ExistsGc();

  drainArgs->ready = true;
}

void progressGcIndexUpdate2(void *arg1, void *arg2)
{
  RAIDController *ctrl = reinterpret_cast<RAIDController*>(arg1);
  ctrl->ProgressGcIndexUpdate();
//  GcTask *task = ctrl->GetGcTask();
//  std::vector<uint64_t> lbas;
//  std::vector<std::pair<PhysicalAddr, PhysicalAddr>> pbas;
//
//  auto it = task->mappings.begin();
//  uint32_t count = 0;
//  while (it != task->mappings.end()) {
//    lbas.emplace_back(it->first);
//    pbas.emplace_back(it->second);
//
//    it = task->mappings.erase(it);
//
//    count += 1;
//    if (count == 256) break;
//  }
//  ctrl->GcBatchUpdateIndex(lbas, pbas);
//  task->stage = INDEX_UPDATING_BATCH;
}

void progressGcIndexUpdate(void *args)
{
  progressGcIndexUpdate2(args, nullptr);
}

