//
// Created by jinho on 5/10/2019.
//

#include "util.h"
namespace sjtu {
    DiskManager userdisk("user.db");
    DiskManager recorddisk("record.db");
    LogManager logManager(&recorddisk);
    BufferPoolManager recordbpm(RECORD_BUFFER_POOL_SIZE , &recorddisk,&userdisk, nullptr, nullptr);
    TransactionManager transactionManager(&logManager,&recordbpm);
}