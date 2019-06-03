/**
 * transaction_manager.h
 *
 */

#pragma once
#include <atomic>
#include <unordered_set>

#include "common/config.h"
#include "transaction.h"
#include "logging/log_manager.h"
#include "buffer/buffer_pool_manager.h"
namespace sjtu {
    class TransactionManager {
    public:
        TransactionManager(LogManager *log_manager = nullptr,BufferPoolManager* bufferPoolManager= nullptr)
                : next_txn_id_(0),
                  log_manager_(log_manager),bufferPoolManager(bufferPoolManager) {}
        Transaction *Begin();
        void Commit(Transaction *txn);
        void Abort(Transaction *txn);

    private:
        std::atomic<txn_id_t> next_txn_id_;
        BufferPoolManager* bufferPoolManager;
        LogManager *log_manager_;
    };

} // namespace sjtu
