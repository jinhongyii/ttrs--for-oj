/**
 * transaction_manager.cpp
 *
 */
#include "concurrency/transaction_manager.h"
#include "table/table_heap.h"

#include <cassert>
namespace sjtu {

    Transaction *TransactionManager::Begin() {
        Transaction *txn = new Transaction(next_txn_id_++);

        if (ENABLE_LOGGING && log_manager_) {
            // begin has no prev_lsn
            LogRecord lg(txn->GetTransactionId(), INVALID_LSN, LogRecordType::BEGIN);
            const lsn_t cur_lsn = log_manager_->AppendLogRecord(lg);

            txn->SetPrevLSN(cur_lsn);
        }

        return txn;
    }
    /**
     * need to delete the pointer
     * @param txn
     */
    void TransactionManager::Commit(Transaction *txn) {
        txn->SetState(TransactionState::COMMITTED);
        // truly delete before commit
        auto record_set = txn->GetRecordSet();
        while (!record_set->empty()) {
            auto &item = record_set->front();
            ;
            if (item.GetLogRecordType() == LogRecordType::MARKDELETE) {
                // this also release the lock when holding the page latch
                auto page= reinterpret_cast<TablePage*>(bufferPoolManager->FetchPage(item.GetDeleteRID().GetPageId() ,
                                                                                     0));
                page->ApplyDelete(item.GetDeleteRID(),txn,log_manager_);
                bufferPoolManager->UnpinPage(item.GetDeleteRID().GetPageId() , true , 0);
            }
            record_set->pop_front();
        }
        record_set->clear();

        if (ENABLE_LOGGING && log_manager_) {
            const lsn_t prev_lsn = txn->GetPrevLSN();
            assert (prev_lsn != INVALID_LSN);
            LogRecord lg(txn->GetTransactionId(), prev_lsn, LogRecordType::COMMIT);
            // commit is the last step in the transaction, we don't need to update
            // the txn's prev lsn at this point
            log_manager_->AppendLogRecord(lg);
            if(log_manager_->GetPersistentLSN()<txn->GetPrevLSN()) {
                log_manager_->flush_all();
            }
            LogRecord txn_end(txn->GetTransactionId(),lg.GetLSN(),LogRecordType::TXN_END);
            log_manager_->AppendLogRecord(txn_end);
        }

    }

    void TransactionManager::Abort(Transaction *txn) {
        txn->SetState(TransactionState::ABORTED);
        // rollback before releasing lock
        auto record_set = txn->GetRecordSet();
        if (ENABLE_LOGGING && log_manager_) {
            const lsn_t prev_lsn = txn->GetPrevLSN();
            assert (prev_lsn != INVALID_LSN);
            LogRecord lg(txn->GetTransactionId(), prev_lsn, LogRecordType::ABORT);
            // abort is the last step in the transaction, we don't need to update
            // the txn's prev lsn at this point
            log_manager_->AppendLogRecord(lg);
        }
        while (!record_set->empty()) {
            auto &item = record_set->back();

            if (item.GetLogRecordType() == LogRecordType::MARKDELETE) {
                auto page= reinterpret_cast<TablePage*>(bufferPoolManager->FetchPage(item.GetDeleteRID().GetPageId() ,
                                                                                     0));
                LOG_DEBUG("rollback delete");
                page->CLRinsert(item.GetDeleteRID() , txn , log_manager_ , item.GetPrevLSN());
                bufferPoolManager->UnpinPage(page->GetPageId() , true , 0);
            } else if (item.GetLogRecordType() == LogRecordType::INSERT) {
                auto page= reinterpret_cast<TablePage*>(bufferPoolManager->FetchPage(item.GetInsertRID().GetPageId() ,
                                                                                     0));
                LOG_DEBUG("rollback insert");
                page->CLRdelete(item.GetInsertRID(),txn,log_manager_,item.GetPrevLSN());
                bufferPoolManager->UnpinPage(page->GetPageId() , true , 0);
            } else if (item.GetLogRecordType() == LogRecordType::UPDATE) {
                auto page= reinterpret_cast<TablePage*>(bufferPoolManager->FetchPage(item.GetUpdateRID().GetPageId() ,
                                                                                     0));
                LOG_DEBUG("rollback update");
                page->CLRupdate(item.GetUpdateRID(),txn,log_manager_,item.GetPrevLSN(),item.GetOldTuple());
                bufferPoolManager->UnpinPage(page->GetPageId() , true , 0);
            }
            record_set->pop_back();
        }
        record_set->clear();
        if(ENABLE_LOGGING&&log_manager_) {
            LogRecord logRecord(txn->GetTransactionId() , txn->GetPrevLSN() , LogRecordType::TXN_END);
            log_manager_->AppendLogRecord(logRecord);
        }

    }
} // namespace sjtu
