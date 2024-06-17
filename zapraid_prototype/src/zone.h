#ifndef __ZONE_H__
#define __ZONE_H__

#include "common.h"
#include "device.h"

class Zone {
public:
  void Init(Device *device, uint64_t slba, uint64_t capacity, uint64_t size);
  void Write(uint32_t offset, uint32_t size, void *ctx);
  void Read(uint32_t offset, uint32_t size, void *ctx);
  void Reset(void *ctx);
  void Seal(void *ctx);
  void Release();

  Device* GetDevice();
  uint32_t GetDeviceId();
  uint32_t GetIssuedPos();
  bool WillBeFull(); // Check mOffset
  bool IsFull(); // Check mPos
  bool NoOngoingWrites();
  uint32_t GetPos();
  void AdvancePos();
  void AdvancePos(uint32_t inc);
  uint32_t GetSlba();
  uint32_t GetLength();
  uint32_t GetSize();

  void PrintStats();
private:
  uint64_t offset2Bytes(uint64_t size);
  Device* mDevice;
  uint64_t mSlba; // in blocks
  uint32_t mCapacity; // in blocks
  uint32_t mSize;
  uint32_t mPos; // in blocks, the finished offset
  uint32_t mOffset; // in blocks, the issued offset
};
#endif
