/**
 * log_manager.cpp
 */

#include "logging/log_manager.h"
#include "common/logger.h"
namespace sjtu {

/*
 * set ENABLE_LOGGING = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
// the flush thread waits for 3 conditions
    // 1. buffer becomes full
    // 2. timeout
    // 3. buffer pool manager evicts a page
    // whose PageLSN > persistent_lsn_

    void LogManager::RunFlushThread () {
        ENABLE_LOGGING = true;
    }

/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
    void LogManager::StopFlushThread () {}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
    lsn_t LogManager::AppendLogRecord (LogRecord &log_record) {
        log_record.lsn_ = next_lsn_++;
        LOG_DEBUG(log_record.ToString().c_str());


        if (bytes_written + log_record.size_ >= LOG_BUFFER_SIZE) {
            flush_all();
        }
        memcpy(log_buffer_ + bytes_written , &log_record , LogRecord::HEADER_SIZE);
        int pos = bytes_written + LogRecord::HEADER_SIZE;
        switch (log_record.log_record_type_) {
            case LogRecordType::INSERT:
                memcpy(log_buffer_ + pos , &log_record.insert_rid_ , sizeof(RID));
                pos += sizeof(RID);
                // we have provided serialize function for tuple class
                log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
                break;
            case LogRecordType::UPDATE:
                memcpy(log_buffer_ + pos , &log_record.update_rid_ , sizeof(RID));
                pos += sizeof(RID);
                log_record.old_tuple_.SerializeTo(log_buffer_ + pos);
                pos += sizeof(int32_t) + log_record.old_tuple_.GetLength();
                log_record.new_tuple_.SerializeTo(log_buffer_ + pos);
                break;
            case LogRecordType::APPLYDELETE:
            case LogRecordType::MARKDELETE:
                memcpy(log_buffer_ + pos , &log_record.delete_rid_ , sizeof(RID));
                pos += sizeof(RID);
                log_record.delete_tuple_.SerializeTo(log_buffer_ + pos);
                break;
            case LogRecordType::NEWPAGE:
                memcpy(log_buffer_ + pos , &log_record.prev_page_id_ , sizeof(page_id_t));
                break;
            case LogRecordType ::CLR:
                memcpy(log_buffer_+pos,&log_record.undo_next,sizeof(lsn_t));
                break;
        }
        bytes_written+=log_record.size_;
        if (log_record.log_record_type_ == LogRecordType::COMMIT) {
            flush_all();
        }
        return log_record.lsn_;
    }

    void LogManager::flush_all () {
        if (bytes_written == 0) {
            return;
        }
        std::swap(flush_buffer_,log_buffer_);
        flush_size=bytes_written;
        disk_manager_->WriteLog(flush_buffer_,flush_size);
        flush_size=0;
        //memset(flush_buffer_,0,LOG_BUFFER_SIZE);
        persistent_lsn_=next_lsn_-1;
        bytes_written=0;
    }

} // namespace sjtu
