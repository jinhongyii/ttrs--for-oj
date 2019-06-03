#include "buffer/buffer_pool_manager.h"

namespace sjtu {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
    BufferPoolManager::BufferPoolManager (size_t pool_size , DiskManager *disk1 , DiskManager *disk2 ,
                                          DiskManager *disk3 , DiskManager *disk4)
            : pool_size_(pool_size)                {
        // a consecutive memory space for buffer pool
        pages_ = new Page[pool_size_];
        page_table_ = new ExtendibleHash<page_id_t , Page *, hash_for_int>(BUCKET_SIZE);
        replacer_ = new LRUReplacer<Page *, hash_for_Page_pointer>(0);
        free_list_ = new sjtu::list<Page *>;
        // put all the pages into free list
        disks[0]=disk1;
        disks[1]=disk2;
        disks[2]=disk3;
        disks[3]=disk4;
        for (size_t i = 0; i < pool_size_; ++i) {
            free_list_->push_back(&pages_[i]);
        }
    }

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
    BufferPoolManager::~BufferPoolManager () {
        FlushAllPages();
        delete[] pages_;
        delete page_table_;
        delete replacer_;
        delete free_list_;

    }

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
    Page *BufferPoolManager::FetchPage (page_id_t page_id , int file_id) {
        Page* page;
        if (page_id == INVALID_PAGE_ID) {
            return nullptr;
        }
        int masked_page_id=page_id+(file_id<<29);
        bool exist=page_table_->Find(masked_page_id,page);
        if (exist) {
            page->pin_count_++;
            if (page->pin_count_ == 1) {
                replacer_->Erase(page);
            }
            return page;
        } else {
            if (!free_list_->empty()) {
                page=free_list_->front();
                free_list_->pop_front();
            } else {
                if (replacer_->Size() == 0) {
                    return nullptr;
                } else {
                    replacer_->Victim(page);
                    if (page->is_dirty_) {
                        disks[page->file_id]->WritePage(page->page_id_,page->data_);
                    }
                    int masked_page_id=page->page_id_+(page->file_id<<29);
                    page_table_->Remove(masked_page_id);
                }
            }
            page_table_->Insert(masked_page_id,page);
            disks[file_id]->ReadPage(page_id, page->data_);
            page->page_id_=page_id;
            page->file_id=file_id;
            page->is_dirty_=0;
            page->pin_count_=1;
            return page;
        }
    }

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer
 * if pin_count<=0 before this call, return false.
 * is_dirty: set the dirty flag of this page
 */
    bool BufferPoolManager::UnpinPage (page_id_t page_id , bool is_dirty , int file_id) {
        Page* page;
        int masked_page_id=page_id+(file_id<<29);
        if(!page_table_->Find(masked_page_id,page)) {
            return false;
        }
        if (page->pin_count_ <= 0) {
            return false;
        } else {
            page->pin_count_--;
            if (page->pin_count_ == 0) {
                replacer_->Insert(page);
            }
            if (is_dirty) {
                page->is_dirty_=true;
            }
            return true;
        }
    }

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
    bool BufferPoolManager::FlushPage (page_id_t page_id , int file_id) {
        Page* page;
        int masked_page_id=page_id+(file_id<<29);
        auto exist=page_table_->Find(masked_page_id , page);
        if (!exist) {
            return false;
        }
//        assert(page_id!=INVALID_PAGE_ID);
        if(page->is_dirty_) {
            disks[file_id]->WritePage(page_id , page->data_);
        }
        page->is_dirty_=0;
        return true;
    }

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
    bool BufferPoolManager::DeletePage (page_id_t page_id , int file_id) {
        Page* page;
        int masked_page_id=page_id+(file_id<<29);
        if (!page_table_->Find(masked_page_id , page)) {
            disks[file_id]->DeallocatePage(page_id);
        } else {
            if (page->pin_count_ != 0) {
                return false;
            } else {
                replacer_->Erase(page);
                page_table_->Remove(masked_page_id);
                free_list_->push_back(page);
                disks[file_id]->DeallocatePage(page_id);
            }
        }
        page->page_id_=INVALID_PAGE_ID;
        return true;

    }

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
    Page *BufferPoolManager::NewPage (page_id_t &page_id , int file_id) {
        page_id=disks[file_id]->AllocatePage();
        char pagedata[PAGE_SIZE]{0};
        disks[file_id]->WritePage(page_id, pagedata);
        int masked_page_id=page_id+(file_id<<29);
        Page* page;
        if (!free_list_->empty()) {
            page=free_list_->front();
            free_list_->pop_front();
        } else {
            if (replacer_->Size() == 0) {
                return nullptr;
            } else {
                replacer_->Victim(page);
                if (page->is_dirty_) {
                    disks[page->file_id]->WritePage(page->page_id_,page->data_);
                }
                int masked_page_id=page->page_id_+(page->file_id<<29);
                page_table_->Remove(masked_page_id);
            }

        }
        page->ResetMemory();
        page->page_id_=page_id;
        page->file_id=file_id;
        page->is_dirty_=0;
        page->pin_count_=1;
        page_table_->Insert(masked_page_id,page);
        return page;
    }

    void BufferPoolManager::FlushAllPages () {
        for (size_t i = 0; i < pool_size_; i++) {
            FlushPage(pages_[i].page_id_ , pages_[i].file_id);
        }
    }

    void BufferPoolManager::clear () {
        page_table_->clear();
        replacer_->clear();
        free_list_->clear();
        for (size_t i = 0; i < pool_size_; i++) {
            free_list_->push_back(&pages_[i]);
        }
    }
} // namespace sjtu
