/**
 * table_heap.cpp
 */

#include <cassert>

#include "common/logger.h"
#include "table/table_heap.h"

namespace sjtu {

// open table
TableHeap::TableHeap(BufferPoolManager *buffer_pool_manager,
                      LogManager *log_manager,
                     page_id_t first_page_id)
    : buffer_pool_manager_(buffer_pool_manager),
      log_manager_(log_manager), first_page_id_(first_page_id) {
    Tuple tuple;
    GetTuple(RID(first_page_id,0),tuple, nullptr);
    tuple.SerializeTo(reinterpret_cast<char*>(&last_page_id));
}

// create table
TableHeap::TableHeap(BufferPoolManager *buffer_pool_manager,
                      LogManager *log_manager,
                     Transaction *txn)
    : buffer_pool_manager_(buffer_pool_manager),
      log_manager_(log_manager) {
  auto first_page =
      static_cast<TablePage *>(buffer_pool_manager_->NewPage(first_page_id_));
  last_page_id=first_page_id_;
  assert(first_page != nullptr); // todo: abort table creation?
  first_page->Init(first_page_id_, PAGE_SIZE, INVALID_LSN, log_manager_, txn);
  Tuple tuple;
  tuple.DeserializeFrom(reinterpret_cast<char*>(&last_page_id),sizeof(last_page_id));
  RID rid;
  InsertTuple(tuple,rid, txn);
//  LOG_DEBUG("new table page created %d", first_page_id_);



  buffer_pool_manager_->UnpinPage(first_page_id_, true);
}



bool TableHeap::InsertTuple(const Tuple &tuple, RID &rid, Transaction *txn) {
  if (tuple.size_ + 32 > PAGE_SIZE) { // larger than one page size
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  auto cur_page =
      static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (cur_page == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }


  while (!cur_page->InsertTuple(
      tuple, rid, txn,
      log_manager_)) { // fail to insert due to not enough space
    auto next_page_id = cur_page->GetNextPageId();
    if (next_page_id != INVALID_PAGE_ID) { // valid next page

      buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
      cur_page = static_cast<TablePage *>(
          buffer_pool_manager_->FetchPage(next_page_id));

    } else { // create new page
      auto new_page =
          static_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      if (new_page == nullptr) {

        buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
        txn->SetState(TransactionState::ABORTED);
        return false;
      }

      // std::cout << "new table page " << next_page_id << " created" <<
      // std::endl;
      cur_page->SetNextPageId(next_page_id);
      new_page->Init(next_page_id, PAGE_SIZE, cur_page->GetPageId(),
                     log_manager_, txn);

      buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), true);
      cur_page = new_page;
      last_page_id=new_page->GetPageId();
      Tuple tuple;
      tuple.DeserializeFrom(reinterpret_cast<char*>(&last_page_id),sizeof(last_page_id));
      RID rid(first_page_id_,0);
      UpdateTuple(tuple,rid, txn);
    }
  }

  buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), true);

  return true;
}

bool TableHeap::MarkDelete(const RID &rid, Transaction *txn) {
  // todo: remove empty page
  auto page = reinterpret_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  page->MarkDelete(rid, txn, log_manager_);

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(const Tuple &tuple, const RID &rid,
                            Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  Tuple old_tuple;

  bool is_updated = page->UpdateTuple(tuple, old_tuple, rid, txn,
                                      log_manager_);

  buffer_pool_manager_->UnpinPage(page->GetPageId(), is_updated);

  return is_updated;
}

void TableHeap::ApplyDelete(const RID &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);

  page->ApplyDelete(rid, txn, log_manager_);


  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}



// called by tuple iterator
bool TableHeap::GetTuple(const RID &rid, Tuple &tuple, Transaction *txn) {
  auto page = static_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  bool res = page->GetTuple(rid, tuple, txn);

  buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
  return res;
}

bool TableHeap::DeleteTableHeap() {
  // todo: real delete
  return true;
}

    bool TableHeap::InsertBack (const Tuple &tuple , RID &rid , Transaction *txn) {
        auto last_page= reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(last_page_id));
        if(last_page->InsertTuple(tuple,rid,txn,log_manager_)) {
            buffer_pool_manager_->UnpinPage(last_page_id,true);
            return true;
        } else {
            page_id_t newpageid;
            auto newPage= reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(newpageid));
            last_page->SetNextPageId(newpageid);
            newPage->Init(newpageid, PAGE_SIZE, last_page_id,
                           log_manager_, txn);
            newPage->InsertTuple(tuple,rid,txn,log_manager_);
            buffer_pool_manager_->UnpinPage(last_page_id, true);
            last_page_id=newpageid;
            Tuple tuple;
            tuple.DeserializeFrom(reinterpret_cast<char*>(&last_page_id),sizeof(last_page_id));
            RID first_rid(first_page_id_,0);
            UpdateTuple(tuple,first_rid, txn);
            buffer_pool_manager_->UnpinPage(newpageid,true);
            return true;
        }

    }

//TableIterator TableHeap::begin(Transaction *txn) {
//  auto page =
//      static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
//
//  RID rid;
//  // if failed (no tuple), rid will be the result of default
//  // constructor, which means eof
//  page->GetFirstTupleRid(rid);
//
//  buffer_pool_manager_->UnpinPage(first_page_id_, false);
//  return TableIterator(this, rid, txn);
//}
//
//TableIterator TableHeap::end() {
//  return TableIterator(this, RID(INVALID_PAGE_ID, -1), nullptr);
//}

} // namespace sjtu
