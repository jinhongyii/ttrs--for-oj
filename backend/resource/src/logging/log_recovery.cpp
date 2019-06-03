/**
 * log_recovey.cpp
 */

#include "logging/log_recovery.h"
#include "page/table_page.h"

namespace sjtu {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data,
                                             LogRecord &log_record) {
    memcpy(&log_record, data, LogRecord::HEADER_SIZE);
    int pos = LogRecord::HEADER_SIZE;
    switch (log_record.log_record_type_) {
        case LogRecordType::INSERT:
            memcpy(&log_record.insert_rid_ , data + pos , sizeof(RID));
            pos += sizeof(RID);
            memcpy(&log_record.size_, data + pos, sizeof(int32_t));
            pos += sizeof(int32_t);
            log_record.insert_tuple_.DeserializeFrom(data + pos, log_record.size_);
            break;
        case LogRecordType::UPDATE:
            memcpy(&log_record.update_rid_ , data + pos , sizeof(RID));
            pos += sizeof(RID);
            memcpy(&log_record.size_, data + pos, sizeof(int32_t));
            pos += sizeof(int32_t);
            log_record.old_tuple_.DeserializeFrom(data + pos, log_record.size_);
            pos += log_record.size_ + sizeof(int32_t);
            log_record.new_tuple_.DeserializeFrom(data + pos, log_record.size_);
            break;
        case LogRecordType::APPLYDELETE:
        case LogRecordType::MARKDELETE:
            memcpy(&log_record.delete_rid_ , data + pos , sizeof(RID));
            pos += sizeof(RID);
            memcpy(&log_record.size_, data + pos, sizeof(int32_t));
            pos += sizeof(int32_t);
            log_record.delete_tuple_.DeserializeFrom(data + pos, log_record.size_);
            break;
        case LogRecordType::NEWPAGE:
            memcpy(&log_record.prev_page_id_ , data + pos , sizeof(page_id_t));
            break;
        case LogRecordType::CLR:
            memcpy(&log_record.undo_next , data + pos , sizeof(lsn_t));
            break;
        case LogRecordType::INSERTKEY:
        case LogRecordType::DELETEKEY:
            memcpy(&log_record.currentPage, data + pos, sizeof(page_id_t));
            pos += sizeof(page_id_t);
            memcpy(&log_record.size_, data + pos, sizeof(int32_t));
            pos += sizeof(int32_t);
            log_record.key.DeserializeFrom(data + pos, log_record.size_);
            break;
        case LogRecordType::INSERTPTR:
        case LogRecordType::DELETEPTR:
            memcpy(&log_record.currentPage, data + pos, sizeof(page_id_t));
            pos += sizeof(page_id_t);
            memcpy(&log_record.ptr, data + pos, sizeof(page_id_t));
            break;
        case LogRecordType ::MOVE:
            memcpy(&log_record.currentPage, data + pos, sizeof(page_id_t));
            pos += sizeof(page_id_t);
            memcpy(&log_record.src, data + pos, sizeof(int));
            pos += sizeof(int);
            memcpy(&log_record.dest, data + pos, sizeof(RID));
            pos += sizeof(RID);
            memcpy(&log_record.size_, data + pos, sizeof(int32_t));
            break;
        case LogRecordType ::DELETEPAGE:
            memcpy(&log_record.currentPage, data + pos, sizeof(page_id_t));
            break;
    }
    return false;
}
/*
    * For INSERTKEY or DELETEKEY type log record
    * -------------------------------------------------------------
    * | HEADER | currentPage | tuple_size | key_tuple_data(char[] array) |
    * -------------------------------------------------------------
    * For INSERTPTR or DELETEPTR type log record
    * -------------------------------------------------------------
    * | HEADER | currentPage | ptr |
    * -------------------------------------------------------------
    * For MOVE type log record
    * --------------------------------------------------------------
    * | HEADER | currentPage | src | dest | size |
    * -------------------------------------------------------------
    * For DELETEPAGE type log record
    * -------------------------------------------------------------
    * | HEADER | currentPage |
    */
/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {
    page_id_t checkerpoint_pos = disk_manager_->get_checkerpoint_pos();
    page_id_t log_file_length = disk_manager_->GetLogSize();
    page_id_t bytes_read_all = log_file_length - checkerpoint_pos;
    page_id_t bytes_read_now = 0;
    while (bytes_read_all >= LOG_BUFFER_SIZE) {

    }
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {}

} // namespace sjtu
