#include <sys/time.h>

#include "debug.h"
#include "spdk/nvme.h"
#include "spdk/nvme_zns.h"
#include <vector>
#include <queue>
#include <map>
#include <set>
#include <mutex>

uint64_t dataset_size = 512 * 1024 * 1024 / 4; // 64GiB / 4KiB

struct timeval g_time_s, g_time_e;
struct timeval t_time_s, t_time_e;
struct spdk_nvme_ctrlr *g_ctrlr = nullptr;
struct spdk_nvme_ns *g_ns = nullptr;
struct spdk_nvme_qpair* g_qpair = nullptr;
std::vector<spdk_nvme_ctrlr*> g_ctrlr_vec;
std::vector<spdk_nvme_ns*> g_ns_vec;
std::vector<spdk_nvme_qpair*> g_qpair_vec;
uint8_t *g_data = nullptr;
uint8_t *g_metadata = nullptr;
uint32_t g_size_accessed = 0, g_num_completed = 0;
uint64_t g_zone_size = 2 * 1024 * 1024 / 4;
uint64_t g_zone_capacity = 1077 * 1024 / 4;
uint32_t g_mode = 0;
uint32_t g_num_concurrent_writes = 1;
uint32_t g_num_devices = 1;
uint32_t g_num_open_zones = 1;
uint32_t g_req_size = 1;
uint32_t g_large_req_size = 4;
//uint64_t g_lba_pointers[4][32];
//uint64_t *g_next_zone_start;
uint64_t start_time_us = 0;
bool g_header_block = false;
bool g_hybrid_num = 0;
bool g_stopped_for_debug = false;
uint64_t g_write_size_gap = 12ull * 1024 * 1024 * 1024 * 1024;  // to simulate the segment manners

std::set<std::string> g_devs;
std::map<uint64_t, std::map<uint64_t, int>> g_zone_issued_size;
std::map<int, std::map<int, uint64_t>> g_finished_zone_sizes;
std::map<int, uint64_t> g_dev_issued_size;
std::map<uint64_t, int> g_finished_size_cnt;
uint64_t g_min_finished_zone_size = 0, g_max_finished_zone_size = 0;
uint64_t g_delayed_size = 0;

struct Context {
  uint64_t* lba;
  uint64_t written_lba;
  uint32_t size;
  struct timeval tv;
  int dev_id;
  int open_zone_id;
  uint8_t* data = nullptr;
  uint8_t* metadata = nullptr;
  uint64_t* next_zone_start = nullptr;
  std::queue<timeval> *start_tvs = nullptr;
};

// delay the fastest device
std::mutex g_lock;
std::set<Context*> g_delayed_ctxs;
std::map<uint64_t, uint64_t> g_time_cnt;
std::map<uint64_t, std::map<uint64_t, uint64_t>> g_dev_time_cnt;

void increment_lba_and_prepare_size(struct Context *ctx); 
void write_block(struct Context *ctx);

static uint64_t round_up(uint64_t a, uint64_t b)
{
  return a / b * b + b;
}

static uint64_t round_down(uint64_t a, uint64_t b)
{
  return a / b * b;
}

static auto probe_cb = [](void *cb_ctx,
    const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr_opts *opts) -> bool {
  return true;
};

static auto attach_cb = [](void *cb_ctx,
    const struct spdk_nvme_transport_id *trid,
    struct spdk_nvme_ctrlr *ctrlr,
    const struct spdk_nvme_ctrlr_opts *opts) -> void {

  if (strcmp(trid->traddr, "0000:63:00.0") == 0 || 
      strcmp(trid->traddr, "0000:64:00.0") == 0 || 
      strcmp(trid->traddr, "0000:65:00.0") == 0 || 
      strcmp(trid->traddr, "0000:66:00.0") == 0) {
    // need to filter
    if (g_devs.find(trid->traddr) != g_devs.end()) { 
      return;
    }

    g_devs.insert(trid->traddr);

    g_ctrlr_vec.push_back(ctrlr);
    g_ns_vec.push_back(spdk_nvme_ctrlr_get_ns(ctrlr, 1));
    g_qpair_vec.push_back(spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, NULL, 0));

    if (strcmp(trid->traddr, "0000:63:00.0") == 0) {
      g_ctrlr = ctrlr;
      g_ns = g_ns_vec[g_ns_vec.size() - 1]; 
      g_qpair = g_qpair_vec[g_qpair_vec.size() - 1];
    }
  }

  return;
};

void complete(void *arg, const struct spdk_nvme_cpl *completion) {
  struct Context *ctx = (Context*)arg;
  static double time = 0.0;
  struct timeval tv, tv_now;

  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "Write zone I/O failed, aborting run "
        "(dev %d open_zone %d zone %lu offset %lu written offset %lu)\n",
        ctx->dev_id, ctx->open_zone_id,
        *ctx->lba / g_zone_size, *ctx->lba % g_zone_size, 
        ctx->written_lba % g_zone_size);
    g_stopped_for_debug = true;
    return;
  }

  // calculate latency; can comment them if you don't like
  {
    gettimeofday(&tv_now, NULL);

    // calculate latency
    if (ctx->start_tvs != nullptr) {
      // single thread for zone write. Don't need to lock
      tv = ctx->start_tvs->front();
      ctx->start_tvs->pop();
    } else {
      tv = ctx->tv;
    }

    uint64_t time_us = (tv_now.tv_sec - tv.tv_sec) * 1000000 + 
      tv_now.tv_usec - tv.tv_usec;
    g_time_cnt[time_us] += 1;
    g_dev_time_cnt[ctx->dev_id][time_us] += 1;
  }

  // calculate the finished sizes
  g_num_completed += ctx->size;

  bool is_delayed = false, is_continued = false;
  uint64_t finished_size = g_finished_zone_sizes[ctx->dev_id][ctx->open_zone_id];
  {
    g_lock.lock();
    assert(g_finished_size_cnt[finished_size] > 0);
    g_finished_size_cnt[finished_size]--;
//    fprintf(stderr, "decrease: finished_size %lu from %u to %u\n", finished_size, 
//        g_finished_size_cnt[finished_size] + 1, g_finished_size_cnt[finished_size]);
    if (g_finished_size_cnt[finished_size] == 0) {
//      fprintf(stderr, "zero %lu!\n", finished_size);
      // change the minimum size
      g_finished_size_cnt.erase(finished_size);
      g_finished_size_cnt[finished_size + ctx->size] += 1;
      if (finished_size == g_min_finished_zone_size) {
        g_min_finished_zone_size = g_finished_size_cnt.begin()->first;
//        fprintf(stderr, "min changed to %lu\n", g_min_finished_zone_size);
      }
    } else {
      g_finished_size_cnt[finished_size + ctx->size] += 1;
    }
//    fprintf(stderr, "increase: finished_size %lu from %u to %u\n", finished_size + ctx->size, 
//        g_finished_size_cnt[finished_size + ctx->size] - 1, g_finished_size_cnt[finished_size + ctx->size]);

    finished_size += ctx->size;
    g_finished_zone_sizes[ctx->dev_id][ctx->open_zone_id] = finished_size;
    g_max_finished_zone_size = std::max(g_max_finished_zone_size, finished_size);
//    fprintf(stderr, "min %lu max %lu current %lu\n", g_min_finished_zone_size, g_max_finished_zone_size, finished_size);

    // if the gap is large, and it is the fastest context, delay it
    if (g_min_finished_zone_size + g_write_size_gap < g_max_finished_zone_size && 
        g_max_finished_zone_size == finished_size) {
      is_delayed = true;
      if (g_delayed_ctxs.size() == 0) {
        g_delayed_size = g_max_finished_zone_size;
      }
    }

    if (g_min_finished_zone_size + g_write_size_gap >= g_delayed_size) {
      is_continued = true;
    }
    g_lock.unlock();
  }

  if (g_size_accessed < dataset_size) {

    // continue the delayed ctx
    if (is_continued) {
      for (auto& it : g_delayed_ctxs) {
//        fprintf(stderr, "continue: size %lu min %lu max %lu\n", finished_size,
//            g_min_finished_zone_size, g_max_finished_zone_size);
        increment_lba_and_prepare_size(it);
        write_block(it);
        g_delayed_ctxs.erase(it);
      }
    }

    // cache delayed ctx
    if (is_delayed) {
      int total = 0;
      for (auto& it : g_finished_size_cnt) {
        total += it.second;
      }
//      fprintf(stderr, "-- cache: size %lu min %lu max %lu mincnt %lu %d total %d\n", finished_size,
//          g_min_finished_zone_size, g_max_finished_zone_size,
//          g_finished_size_cnt.begin()->first, g_finished_size_cnt.begin()->second, total);
      g_delayed_ctxs.insert(ctx);
    } else {
      increment_lba_and_prepare_size(ctx);
      write_block(ctx);
    }
  }

  gettimeofday(&g_time_e, NULL);
  double elapsed = g_time_e.tv_sec - g_time_s.tv_sec 
    + g_time_e.tv_usec / 1000000. - g_time_s.tv_usec / 1000000.;
  if (elapsed - time > 3.0) {
    printf("Throughput: %.6fMiB/s", g_num_completed * 4ull / 1024 / elapsed);
    for (auto& it : g_dev_issued_size) {
      printf(" (%.2f)", it.second * 4ull / 1024 / elapsed);
    } 
    printf("\n");
    time = elapsed;
  }
};

void increment_lba_and_prepare_size(struct Context *ctx) {
  // increment LBA
  *ctx->lba += ctx->size;
  if (*ctx->lba / g_zone_size == ctx->written_lba / g_zone_size &&
      g_zone_issued_size[ctx->dev_id][ctx->written_lba / g_zone_size] == g_zone_capacity) 
  {
    *ctx->lba = *ctx->next_zone_start;
    *ctx->next_zone_start += g_zone_size;
  }

  // prepare size
  if (g_header_block) {
    if (*ctx->lba % g_zone_size == 0) {
      ctx->size = 1;
    } else if (g_mode <= 2 || *ctx->lba / g_zone_size % g_num_open_zones <= g_hybrid_num) {
      if (g_zone_capacity - *ctx->lba % g_zone_size < g_req_size) { 
        ctx->size = g_req_size - 1;
      } else {
        ctx->size = g_req_size;
      }
    } else {
      if (g_zone_capacity - *ctx->lba % g_zone_size < g_large_req_size) { 
        ctx->size = g_large_req_size - 1;
      } else {
        ctx->size = g_large_req_size;
      }
    }
  } 
  // else, does not need to modify
//  else if (g_mode <= 2 || ctx->lba / g_zone_size % g_num_open_zones <= g_hybrid_num) {
//    ctx->size = g_req_size;
//  } else {
//    ctx->size = g_large_req_size;
//  }
}

void write_block(struct Context *ctx)
{
  // zone append (left) or zone write (right, all concurrent writes are issued)

  // record the start time
  if (g_stopped_for_debug) {
    return;
  }
  gettimeofday(&ctx->tv, NULL);

  if (ctx->start_tvs != nullptr) {
    // is zone write
    while (ctx->start_tvs->size() < g_num_concurrent_writes) {
      ctx->start_tvs->push(ctx->tv);
    }
    if (ctx->start_tvs->empty()) {
      return;
    }
  }

  if (ctx->size == 0) {
    printf("size == 0\n");
    exit(1);
  }

  g_size_accessed += ctx->size;

  auto qpair = g_qpair_vec[ctx->dev_id];
  auto ns = g_ns_vec[ctx->dev_id];

  if (g_mode == 0) {
    int rc;
    ctx->written_lba = *ctx->lba;
    g_zone_issued_size[ctx->dev_id][ctx->written_lba / g_zone_size] += ctx->size;
    g_dev_issued_size[ctx->dev_id] += ctx->size;
    if (ctx->metadata == nullptr) {
      rc = spdk_nvme_ns_cmd_write(
          ns, qpair,
          ctx->data, *ctx->lba, ctx->size,
          complete, ctx, 0);
    } else {
      rc = spdk_nvme_ns_cmd_write_with_md(
          ns, qpair,
          ctx->data, ctx->metadata, *ctx->lba, ctx->size,
          complete, ctx, 0, 0, 0);
    }
    if (rc < 0) {
      printf("write error.");
    }
  } else if (g_mode == 1) {
    uint64_t zslba = round_down(*ctx->lba, g_zone_size);
    int rc;
    ctx->written_lba = *ctx->lba;
    g_zone_issued_size[ctx->dev_id][ctx->written_lba / g_zone_size] += ctx->size;
    g_dev_issued_size[ctx->dev_id] += ctx->size;
    if (ctx->metadata == nullptr) {
      rc = spdk_nvme_zns_zone_append(
          ns, qpair,
          ctx->data, zslba, ctx->size, 
          complete, ctx, 0);
    } else {
      rc = spdk_nvme_zns_zone_append_with_md(
          ns, qpair,
          ctx->data, ctx->metadata, zslba, ctx->size, 
          complete, ctx, 0, 0, 0);
    }
    if (rc < 0) {
      printf("append error.");
    }
  } else if (g_mode == 2) { 
    uint64_t zslba = round_down(*ctx->lba, g_zone_size); 
    uint32_t zone_num = zslba / g_zone_size;
    if (zone_num % g_num_open_zones > g_hybrid_num) {
      int rc;
      rc = spdk_nvme_ns_cmd_write(
          ns, qpair,
          ctx->data, *ctx->lba, ctx->size,
          complete, ctx, 0);
      if (rc < 0) {
        printf("write error.");
      }
    } else {
      int rc;
      rc = spdk_nvme_zns_zone_append(
          ns, qpair,
          ctx->data, zslba, ctx->size, 
          complete, ctx, 0);
      if (rc < 0) {
        printf("append error.");
      }
    }
  } else if (g_mode == 3) {
    uint64_t zslba = round_down(*ctx->lba, g_zone_size); 
    uint32_t zone_num = zslba / g_zone_size;
    if (zone_num % g_num_open_zones > g_hybrid_num) {
      int rc;
      rc = spdk_nvme_ns_cmd_write(
          ns, qpair,
          ctx->data, *ctx->lba, ctx->size, 
          complete, ctx, 0);
      if (rc < 0) {
        printf("write error.");
      }
    } else {
      int rc;
      rc = spdk_nvme_zns_zone_append(
          ns, qpair,
          ctx->data, zslba, ctx->size, 
          complete, ctx, 0);
      if (rc < 0) {
        printf("append error.");
      }
    }

  } else if (g_mode == 4) {
    uint64_t zslba = round_down(*ctx->lba, g_zone_size); 
    uint32_t zone_num = zslba / g_zone_size;
    int rc;
    rc = spdk_nvme_ns_cmd_write(
        ns, qpair,
        ctx->data, *ctx->lba, ctx->size,
        complete, ctx, 0);
    if (rc < 0) {
      printf("write error.");
    }
  }

  // started, push into the ongoing queue
}

void init_for_write(int zone_num, uint32_t req_size, int dev_id, struct Context *ctx, uint64_t* zone_start, uint64_t* lba_ptr) {
  int lt_idx = zone_num * g_num_concurrent_writes;
  struct timeval tv;

  *lba_ptr = zone_num * g_zone_size;
//  g_lba_pointers[dev_id][zone_num] = zone_num * g_zone_size;
  ctx[lt_idx].lba = lba_ptr; 
    //&(g_lba_pointers[dev_id][zone_num]);
  ctx[lt_idx].start_tvs = new std::queue<timeval>();
  ctx[lt_idx].dev_id = dev_id;
  ctx[lt_idx].open_zone_id = zone_num;
  ctx[lt_idx].next_zone_start = zone_start;
  ctx[lt_idx].data = g_data + (4096 * g_large_req_size) * (dev_id *
      g_num_open_zones * g_num_concurrent_writes + lt_idx);
  ctx[lt_idx].metadata = g_metadata ? g_metadata + (64 * g_large_req_size) *
    (dev_id * g_num_open_zones * g_num_concurrent_writes + lt_idx) : nullptr;

  g_finished_size_cnt[0]++;
  g_finished_zone_sizes[dev_id][zone_num] = 0;
  
//  fprintf(stderr, "init dev %d lba %lu ns %p qpair %p\n", 
//      dev_id, ctx[lt_idx].lba, 
//      g_ns_vec[dev_id], g_qpair_vec[dev_id]);

  gettimeofday(&tv, NULL);
  if (g_header_block) {
    ctx[lt_idx].size = 1;
  } else {
    ctx[lt_idx].size = req_size;
  }
  for (uint32_t j = 0; j < g_num_concurrent_writes; ++j) {
    ctx[lt_idx].start_tvs->push(tv);
  }
  write_block(ctx + lt_idx);
}

void init_for_append(int zone_num, uint32_t req_size, int dev_id, struct Context *ctx, uint64_t* zone_start, uint64_t* lba_ptr) {
  *lba_ptr = zone_num * g_zone_size;
  for (uint32_t j = 0; j < g_num_concurrent_writes; ++j) {
    int lt_idx = zone_num * g_num_concurrent_writes + j;
    // TODO breaks when the header is enabled
//    g_lba_pointers[zone_num] = zone_num * g_zone_size + j * req_size;
    ctx[lt_idx].lba = lba_ptr; 
    *lba_ptr += req_size;
      //&g_lba_pointers[zone_num];
    ctx[lt_idx].size = req_size;
    ctx[lt_idx].dev_id = dev_id;
    ctx[lt_idx].open_zone_id = zone_num;
    ctx[lt_idx].next_zone_start = zone_start;
    ctx[lt_idx].data = g_data + (4096 * g_large_req_size) * (dev_id *
        g_num_open_zones * g_num_concurrent_writes + lt_idx);
    ctx[lt_idx].metadata = g_metadata ? g_metadata + (64 * g_large_req_size) *
      (dev_id * g_num_open_zones * g_num_concurrent_writes + lt_idx) : nullptr;
    write_block(ctx + lt_idx);
  }
  g_finished_size_cnt[0]++;
  g_finished_zone_sizes[dev_id][zone_num] = 0;
}

int main(int argc, char *argv[])
{
  setbuf(stdout, NULL);
  int opt;
  while ((opt = getopt(argc, argv, "m:c:z:s:h:l:t:y:d:g:")) != -1) {  // for each option...
    switch (opt) {
      case 'm':
        g_mode = atoi(optarg);
        break;
      case 'c':
        g_num_concurrent_writes = atoi(optarg);
        break;
      case 'z':
        g_num_open_zones = atoi(optarg);
        break;
      case 's':
        g_req_size = atoi(optarg);
        if (g_large_req_size < g_req_size) {
          g_large_req_size = g_req_size;
        }
        break;
      case 'l':
        g_large_req_size = atoi(optarg);
        break;
      case 'd':
        g_num_devices = atoi(optarg);
        break;
      case 'y':
        g_hybrid_num = atoi(optarg);
        break;
      case 't': // traffic size
        dataset_size = atoi(optarg) * 1024 * 256;
        break; 
      case 'g': // gap
        g_write_size_gap = atoi(optarg);
        break;
      case 'h':
      default:
        printf(
            "-m: write primitive, 0 - use zone write, 1 - use zone append\n"
            "                     2 - mix zone write and zone append (same size)\n"
            "                     3 - mix zone write and zone append (diff size)\n"
            "                     4 - mix zone writes with different sizes\n"
            "-c: number of concurrent write requests\n"
            "-z: number of open zones\n"
            "-s: request size\n"
            "-l: large request size\n"
            "-d: header block\n");
        return 0;
    }
  }

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

  assert(g_ns_vec.size() >= g_num_devices);

  auto resetComplete = [](void *arg, const struct spdk_nvme_cpl *completion) {
    bool *done = (bool*)arg;
    *done = true;
  };

  for (int dev_id = 0; dev_id < g_num_devices; ++dev_id) {
    auto ns = g_ns_vec[dev_id];
    auto qpair = g_qpair_vec[dev_id];

    if (ns == nullptr) {
      debug_e("device not found: please bound the device first!");
      exit(1);
    }

    done = false;
    if (spdk_nvme_zns_reset_zone(ns, qpair, 0, true, resetComplete, &done) < 0) {
      printf("Reset zone failed.\n");
      return 0;
    }
    while (!done) {
      spdk_nvme_qpair_process_completions(qpair, 0);
    }
  }

  printf("Mode: %d, Concurrent writes: %d, Open zones: %d, Request size: %d\n",
      g_mode, g_num_concurrent_writes, g_num_open_zones, g_req_size);

  uint32_t num_data_blocks = g_large_req_size * g_num_open_zones *
    g_num_concurrent_writes * g_num_devices;
  g_data = (uint8_t*)spdk_zmalloc(4096 * num_data_blocks, 4096, NULL,
      SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
  for (uint32_t i = 0; i < 4096 * num_data_blocks; ++i) {
    g_data[i] = rand() % 256;
  }

  if (spdk_nvme_ns_get_md_size(g_ns) > 0) {
    g_metadata = (uint8_t*)spdk_zmalloc(64 * num_data_blocks, 4096, NULL,
        SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    for (uint32_t i = 0; i < 64 * num_data_blocks; ++i) {
      g_metadata[i] = rand() % 256;
    }
  }

  gettimeofday(&g_time_s, NULL);

  struct Context **ctx;
  start_time_us = g_time_s.tv_sec * 1000000 + g_time_s.tv_usec;

  // initialize

  uint64_t next_zone_start[g_num_devices];
  ctx = new struct Context*[g_num_devices];
  uint64_t lba_pool[g_num_devices * g_num_open_zones];

  for (uint32_t nd = 0; nd < g_num_devices; ++nd) {
    next_zone_start[nd] = g_num_open_zones * g_zone_size;
    ctx[nd] = new struct Context[g_num_open_zones * g_num_concurrent_writes];

    for (uint32_t i = 0; i < g_num_open_zones; ++i) {
      if (g_mode == 0) {
        init_for_write(i, g_req_size, nd, ctx[nd], &next_zone_start[nd],
            lba_pool + nd * g_num_open_zones + i);
      } else if (g_mode == 1) {
        init_for_append(i, g_req_size, nd, ctx[nd], &next_zone_start[nd],
            lba_pool + nd * g_num_open_zones + i);
      } else if (g_mode == 2) {
        // mod open_zone_num == 0 for append, others for write 
        if (i <= g_hybrid_num) {
          init_for_append(i, g_req_size, nd, ctx[nd], &next_zone_start[nd],
              lba_pool + nd * g_num_open_zones + i);
        } else {
          init_for_write(i, g_req_size, nd, ctx[nd], &next_zone_start[nd],
              lba_pool + nd * g_num_open_zones + i);
        }
      } else if (g_mode == 3) {
        // mod open_zone_num == 0 for append, others for write (large) 
        if (i <= g_hybrid_num) {
          init_for_append(i, g_req_size, nd, ctx[nd], &next_zone_start[nd], 
              lba_pool + nd * g_num_open_zones + i);
        } else {
          init_for_write(i, g_large_req_size, nd, ctx[nd], &next_zone_start[nd], 
              lba_pool + nd * g_num_open_zones + i);
        }
      } else if (g_mode == 4) {
        // mod open_zones == 0 for write (small) 
        // mod open_zones > 0 for write (large) 
        if (i <= g_hybrid_num) {
          init_for_write(i, g_req_size, nd, ctx[nd], &next_zone_start[nd], 
              lba_pool + nd * g_num_open_zones + i);
        } else {
          init_for_write(i, g_large_req_size, nd, ctx[nd], &next_zone_start[nd], 
              lba_pool + nd * g_num_open_zones + i);
        }
      }
    }
  }

  while (g_num_completed < dataset_size) { // 64GiB / 4KiB
    if (g_stopped_for_debug) break;
    // report the performance and exit the program
    for (int i = 0; i < g_num_devices; i++) {
      spdk_nvme_qpair_process_completions(g_qpair_vec[i], 0);
    }
  }

  if (g_stopped_for_debug) {
    for (int nd = 0; nd < g_num_devices; ++nd) {
      for (uint32_t i = 0; i < g_num_open_zones * g_num_concurrent_writes; ++i) {
        fprintf(stderr, "dev %d %d ctrlr %p ns %p qpair %p lba %lu written %lu size %d\n",
            nd, ctx[nd][i].dev_id,
            g_ctrlr_vec[nd], g_ns_vec[nd], g_qpair_vec[nd],
            *ctx[nd][i].lba, ctx[nd][i].written_lba, ctx[nd][i].size); 
      }
    }
    for (auto& it : g_zone_issued_size) {
      for (auto& it0 : it.second) {
        fprintf(stderr, "dev %lu zone %lu cnt %d\n", it.first, it0.first, it0.second);
      }
    }
    return 0;
  }

  {
    uint64_t total_cnt = 0, total_time = 0;
    for (auto it = g_time_cnt.begin(); it != g_time_cnt.end(); ++it) {
      total_cnt += it->second;
      total_time += it->first * it->second;
    }

    std::vector<double> percentiles = {50, 90, 95, 99, 99.9, 99.99};
    std::map<int, int> printed;
    // calculate the percentiles 
    uint64_t cnt = 0;
    for (auto it = g_time_cnt.begin(); it != g_time_cnt.end(); ++it) {
      cnt += it->second;
      for (int i = 0; i < percentiles.size(); ++i) {
        auto p = percentiles[i];
        if (cnt >= total_cnt * p / 100 && !printed.count(i)) {
          printf("p%.2f: %lu\n", p, it->first);
          printed[i] = 1;
          break;
        }
      }
    }
    printf("avg: %.2lf\n", (double)total_time / total_cnt);
  }

  {
    for (auto& it : g_dev_time_cnt) {
      uint64_t total_cnt = 0, total_time = 0;
      for (auto it0 = it.second.begin(); it0 != it.second.end(); ++it0) {
        total_cnt += it0->second;
        total_time += it0->first * it0->second;
      }

      std::vector<double> percentiles = {50, 90, 95, 99, 99.9, 99.99};
      std::map<int, int> printed;
      // calculate the percentiles 
      uint64_t cnt = 0;
      for (auto it0 = it.second.begin(); it0 != it.second.end(); ++it0) {
        cnt += it0->second;
        for (int i = 0; i < percentiles.size(); ++i) {
          auto p = percentiles[i];
          if (cnt >= total_cnt * p / 100 && !printed.count(i)) {
            printf("dev %lu p%.2f: %lu\n", it.first, p, it0->first);
            printed[i] = 1;
            break;
          }
        }
      }
      printf("dev %lu avg: %.2lf\n", it.first, (double)total_time / total_cnt);
    }
  }

  for (int i = 0; i < g_num_devices; i++) {
    for (int j = 0; j < g_num_open_zones * g_num_concurrent_writes; j++) {
      if (ctx[i][j].start_tvs != nullptr) {
        delete ctx[i][j].start_tvs;
      }
    }
    delete[] ctx[i];
  }
  delete[] ctx;
  spdk_free(g_data);
  if (g_metadata) {
    spdk_free(g_metadata);
  }

  gettimeofday(&g_time_e, NULL);
  double elapsed = g_time_e.tv_sec - g_time_s.tv_sec 
    + g_time_e.tv_usec / 1000000. - g_time_s.tv_usec / 1000000.;
  printf("Throughput: %.6fMiB/s\n", dataset_size * 4ull / 1024 / elapsed);

  for (auto& it : g_qpair_vec) {
    spdk_nvme_ctrlr_free_io_qpair(it); 
  }
}
