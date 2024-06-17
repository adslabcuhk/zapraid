#include "zone.h"
#include <isa-l.h>



inline uint64_t Zone::offset2Bytes(uint64_t offset)
{
  return (mSlba << 12) + offset * Configuration::GetBlockSize();
}

void Zone::Init(Device* device, uint64_t slba, uint64_t capacity, uint64_t size)
{
  mDevice = device;
  mSlba = slba;
  mCapacity = capacity;
  mSize = size;
  mOffset = 0; // the in-zone offset of issued I/O requests
  mPos = 0;    // the in-zone offset of finished I/O requests
}

// size: in bytes
void Zone::Write(uint32_t offset, uint32_t size, void *ctx)
{
  RequestContext *reqCtx = (RequestContext*)ctx;
  debug_warn("this %p slba %lu, offset %u, size %u, mPos %u\n", this, mSlba, offset, size, mPos);
  if (reqCtx->append) {
    reqCtx->offset = mSize - 1;
    mDevice->Append(offset2Bytes(0), size, ctx);
  } else {
    reqCtx->offset = offset;
    mDevice->Write(offset2Bytes(offset), size, ctx);
  }
  mOffset += size / Configuration::GetBlockSize();
}

void Zone::Read(uint32_t offset, uint32_t size, void *ctx)
{
  mDevice->Read(offset2Bytes(offset), size, ctx);
}

void Zone::Reset(void *ctx)
{
  mOffset = 0;
  mPos = 0;
  mDevice->ResetZone(this, ctx);
}

void Zone::Seal(void *ctx)
{
  mDevice->FinishZone(this, ctx);
}

Device* Zone::GetDevice()
{
  return mDevice;
}

uint32_t Zone::GetDeviceId()
{
  return mDevice->GetDeviceId();
}

uint32_t Zone::GetIssuedPos() {
  return mOffset;
}

bool Zone::WillBeFull() {
  return (mOffset + 32 >= mCapacity);  // reserve 32 blocks
}

uint32_t Zone::GetPos()
{
  return mPos;
}

bool Zone::IsFull() {
  return (mPos >= mCapacity);
}

bool Zone::NoOngoingWrites() {
  return mOffset == mPos;
}

void Zone::AdvancePos()
{
  // not thread safe. Add lock when using multiple I/O threads
  mPos += 1;
}

void Zone::AdvancePos(uint32_t inc) 
{
  // not thread safe. Add lock when using multiple I/O threads
  mPos += inc;
}

uint32_t Zone::GetSlba()
{
  return mSlba;
}

uint32_t Zone::GetLength()
{
  return mCapacity;
}

uint32_t Zone::GetSize()
{
  return mSize;
}

void Zone::PrintStats()
{
  printf("device id: %d, slba: %lu, length: %d\n",
      GetDeviceId(), mSlba, mCapacity);
}

void Zone::Release()
{
  mDevice->AddAvailableZone(this);
}
