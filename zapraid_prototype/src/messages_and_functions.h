#ifndef __MESSAGES_AND_FUNCTIONS_H__
#define __MESSAGES_AND_FUNCTIONS_H__

#include "common.h"

struct DrainArgs {
  RAIDController *ctrl;
  bool success;
  bool ready;
};

struct UpdatePbaArgs {
  RAIDController *ctrl;
  RequestContext *ctx;
};

struct QueryPbaArgs {
  RAIDController *ctrl;
  RequestContext *ctx;
};

void tryDrainController(void *args);

// Used under spdk thread library
void handleEventCompletion(void *args);
int ioWorker(void *args);
int dispatchWorker(void *args);
int ecWorker(void *args);
int indexWorker(void *args);
int completionWorker(void *args);

void enqueueRequest(void *args);
void enqueueIndexRequest(void *args);
void queryPba(void *args);
void updatePba(void *args);
void handleIndexContextMsg(void *args);

// Used under spdk event library
void executeRequest(void *arg1, void *arg2);

void registerIoCompletionRoutine(void *arg1, void *arg2);
void registerDispatchRoutine(void *arg1, void *arg2);
void handleEventCompletion2(void *arg1, void *arg2);

void enqueueRequest2(void *arg1, void *arg2);
void enqueueIndexRequest2(void *arg1, void *arg2);
void queryPba2(void *arg1, void *arg2);
void updatePba2(void *arg1, void *arg2);
void handleIndexContextMsg2(void *arg1, void *arg2);

// handle zone writes
bool checkPendingWrites(RAIDController* ctrl); 
int handleIoPendingZoneWrites(void *args);
void realZoneWrite(RequestContext *args);
void bufferCopy2(void *arg1, void *arg2);
void bufferCopy(void *args);

// For devices
void zoneWrite2(void *arg1, void *arg2);
void zoneRead2(void *arg1, void *arg2);
void zoneAppend2(void *arg, void *arg2);
void zoneReset2(void *arg, void *arg2);
void zoneFinish2(void *arg, void *arg2);

void zoneWrite(void *args);
void zoneRead(void *args);
void zoneAppend(void *args);
void zoneReset(void *args);
void zoneFinish(void *args);

// GC
void progressGcIndexUpdate2(void *arg1, void *arg2);
void progressGcIndexUpdate(void *args);

#endif
