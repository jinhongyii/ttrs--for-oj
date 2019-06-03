/**
 * config.h
 *
 * Database system configuration
 */

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

namespace sjtu {

extern std::chrono::duration<long long int> LOG_TIMEOUT;

extern bool ENABLE_LOGGING;

#define INVALID_PAGE_ID -1 // representing an invalid page id
#define INVALID_TXN_ID -1  // representing an invalid txn id
#define INVALID_LSN -1     // representing an invalid lsn
#define HEADER_PAGE_ID 0   // the header page id
#define PAGE_SIZE 8192   // size of a data page in byte
#define BUFFER_SIZE 409600
#define LOG_BUFFER_SIZE                                                            \
  ((RECORD_BUFFER_POOL_SIZE + 1) * PAGE_SIZE) // size of a log buffer in byte
#define BUCKET_SIZE 50                 // size of extendible hash bucket
#define RECORD_BUFFER_POOL_SIZE 1300            // size of buffer pool

typedef int32_t page_id_t; // page id type
typedef int32_t txn_id_t;  // transaction id type
typedef int32_t lsn_t;     // log sequence number type

} // namespace sjtu