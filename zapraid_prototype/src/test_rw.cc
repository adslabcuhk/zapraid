#include <sys/time.h>

#include "debug.h"
#include "spdk/nvme.h"
#include "spdk/nvme_zns.h"
#include <vector>
#include <queue>
#include <map>
#include <set>
#include <mutex>

uint64_t dataset_size = 64 * 1024 * 1024 / 4; // 64GiB / 4KiB

struct timeval g_time_s, g_time_e;
struct timeval t_time_s, t_time_e;
struct spdk_nvme_ctrlr *g_ctrlr = nullptr;
struct spdk_nvme_ns *g_ns = nullptr;
struct spdk_nvme_qpair* g_qpair = nullptr;
uint8_t *g_data = nullptr;
uint32_t g_num_issued = 0, g_num_completed = 0;
uint64_t g_zone_size = 2 * 1024 * 1024 / 4;
uint64_t g_zone_capacity = 1077 * 1024 / 4;
uint32_t g_mode = 0;
uint32_t g_num_concurrent_writes = 1;
uint32_t g_num_open_zones = 1;
uint32_t g_req_size = 1;
uint32_t g_large_req_size = 1; //8; //4;
uint64_t g_lba_pointers[32];
uint64_t *g_next_zone_start;
uint64_t g_rated_thpt_mbs = 0;
uint64_t g_rated_interval = 50;
uint64_t start_time_us = 0;
uint64_t g_ongoing_cnt = 0;
bool g_header_block = false;
bool g_hybrid_num = 0;

static auto probe_cb = [](void *cb_ctx,
    const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr_opts *opts) -> bool {
  return true;
};

static auto attach_cb = [](void *cb_ctx,
    const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr *ctrlr,
    const struct spdk_nvme_ctrlr_opts *opts) -> void {

  if ((strcmp(trid->traddr, "0000:00:09.0") == 0 || strcmp(trid->traddr,
                  "0000:65:00.0") == 0) && g_ctrlr == nullptr) {
    g_ctrlr = ctrlr;
    g_ns = spdk_nvme_ctrlr_get_ns(ctrlr, 1);

    g_qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, NULL, 0);
  }

  return;
};

void complete(void *arg, const struct spdk_nvme_cpl *completion) {
  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "Write zone I/O failed\n");
    exit(1);
  }
  bool *done = (bool*)arg;
  *done = true;
}

int main(int argc, char *argv[])
{
  setbuf(stdout, NULL);
  int opt;
  int ret = 0;

  struct spdk_env_opts opts;
  spdk_env_opts_init(&opts);
  if (spdk_env_init(&opts) < 0) {
    fprintf(stderr, "Unable to initialize SPDK env.\n");
    exit(-1);
  }

  ret = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL); 
  if (ret < 0) {
    fprintf(stderr, "Unable to probe devices\n");
    exit(-1);
  }

  bool done = false;

  if (g_ns == nullptr) {
    debug_e("device not found: please bound the device first!");
    exit(1);
  }

  // reset
  auto resetComplete = [](void *arg, const struct spdk_nvme_cpl *completion) {
    bool *done = (bool*)arg;
    *done = true;
  };

  bool rewrite = true;//false;

  if (rewrite) {
    if (spdk_nvme_zns_reset_zone(g_ns, g_qpair, 0, true, resetComplete, &done) < 0) {
      printf("Reset zone failed.\n");
    }

    while (!done) {
      spdk_nvme_qpair_process_completions(g_qpair, 0);
    }
  }

  // write
  g_data = (uint8_t*)spdk_zmalloc(4096 * g_large_req_size, 4096, NULL,
      SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
  for (uint32_t i = 0; i < 4096 * g_large_req_size; ++i) {
    g_data[i] = rand() % 256;
  }

  if (rewrite) {
    for (int i = 0; i < g_large_req_size; i++) {
      fprintf(stderr, "write [%lx] [%lx] [%lx]\n", 
          *((uint64_t*)(g_data + i * 4096)),
          *((uint64_t*)(g_data + i * 4096 + 512)), 
          *((uint64_t*)(g_data + i * 4096 + 1024))); 
    }
  }

  uint8_t* meta = (uint8_t*)spdk_zmalloc(64 * g_large_req_size, 4096, NULL,
      SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);

  for (uint32_t i = 0; i < 64 * g_large_req_size; ++i) {
    meta[i] = rand() % 256;
  }

  if (rewrite) {
    for (int i = 0; i < g_large_req_size; i++) {
      fprintf(stderr, "write meta [%lx] [%lx] [%lx]\n", 
          *((uint64_t*)(meta + i * 64)),
          *((uint64_t*)(meta + i * 64 + 8)), 
          *((uint64_t*)(meta + i * 64 + 16))); 
    }
  }

  int rc = 0;

  struct timeval tv1, tv2;
  gettimeofday(&tv1, 0);
  if (rewrite) {
//    for (int lba = 0; lba < 1024 * 1024 / 4; lba += g_large_req_size) 
    for (int lba = 0; lba < 1024 * 256 / 4; lba += g_large_req_size) 
    {
      done = false;
      rc = spdk_nvme_ns_cmd_write_with_md(g_ns, g_qpair, g_data, meta, 
          lba, g_large_req_size, complete, &done, 0, 0, 0);

      if (rc != 0) {
        fprintf(stderr, "write failed rc %d for lba %d\n", rc, lba);
        exit(1);
      }

      while (!done) {
        spdk_nvme_qpair_process_completions(g_qpair, 0);
      }
    }
  }
  gettimeofday(&tv2, 0);
  printf("write time: %.2lf s\n", (double)(tv2.tv_sec - tv1.tv_sec) +
      (tv2.tv_usec - tv1.tv_usec) / 1000000.0);

  // read
  uint8_t* read_data = (uint8_t*)spdk_zmalloc(4096 * g_large_req_size, 4096, NULL,
      SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
  uint8_t* read_meta = (uint8_t*)spdk_zmalloc(64 * g_large_req_size, 4096, NULL,
      SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);

  done = false;
  rc = spdk_nvme_ns_cmd_read_with_md(g_ns, g_qpair, read_data, read_meta, 
      0, g_large_req_size, complete, &done, 0, 0, 0);
  printf("rc: %d\n", rc);
  printf("metadata size: %d\n", spdk_nvme_ns_get_md_size(g_ns));
  printf("sector size: %d\n", spdk_nvme_ns_get_sector_size(g_ns));
  printf("extended sector size: %d\n", spdk_nvme_ns_get_extended_sector_size(g_ns));
  printf("zone size in sectors: %ld\n", spdk_nvme_zns_ns_get_zone_size_sectors(g_ns));
  printf("zone size: %ld\n", spdk_nvme_zns_ns_get_zone_size(g_ns));

  while (!done) {
    spdk_nvme_qpair_process_completions(g_qpair, 0);
  }

  for (int i = 0; i < g_large_req_size; i++) {
    fprintf(stderr, "read [%lx] [%lx] [%lx]\n", 
        *((uint64_t*)(read_data + i * 4096)),
        *((uint64_t*)(read_data + i * 4096 + 512)),
        *((uint64_t*)(read_data + i * 4096 + 1024)));
  }
  for (int i = 0; i < g_large_req_size; i++) {
    fprintf(stderr, "read meta [%lx] [%lx] [%lx] [%lx] [%lx] [%lx] [%lx] [%lx]\n", 
        *((uint64_t*)(read_meta + i * 64)),
        *((uint64_t*)(read_meta + i * 64 + 8)),
        *((uint64_t*)(read_meta + i * 64 + 16)),
        *((uint64_t*)(read_meta + i * 64 + 24)),
        *((uint64_t*)(read_meta + i * 64 + 32)),
        *((uint64_t*)(read_meta + i * 64 + 40)),
        *((uint64_t*)(read_meta + i * 64 + 48)),
        *((uint64_t*)(read_meta + i * 64 + 56)));
  }
  return 0;
}
