/*
 * buffer_pool_manager.h
 *
 * Functionality: The simplified Buffer Manager interface allows a client to
 * new/delete pages on disk, to read a disk page into the buffer pool and pin
 * it, also to unpin a page in the buffer pool.
 */

#pragma once
#include <list>
#include <mutex>

#include "buffer/lru_replacer.h"
#include "disk/disk_manager.h"
#include "hash/extendible_hash.h"

#include "page/page.h"
#include "hash/hash_method.h"

namespace sjtu {
    class BufferPoolManager {
    public:
        BufferPoolManager (size_t pool_size , DiskManager *disk1 , DiskManager *disk2 ,
                           DiskManager *disk3 , DiskManager *disk4);

        ~BufferPoolManager();

        Page *FetchPage (page_id_t page_id , int file_id);

        bool UnpinPage (page_id_t page_id , bool is_dirty , int file_id);

        bool FlushPage (page_id_t page_id , int file_id);

        Page *NewPage (page_id_t &page_id , int file_id);

        bool DeletePage (page_id_t page_id , int file_id);

        void FlushAllPages();

        void clear();

        static const int p = 2003;

        DiskManager* disks[4];

    private:

        size_t pool_size_; // number of pages in buffer pool
        Page *pages_;      // array of pages

        HashTable<page_id_t, Page *, hash_for_int> *page_table_; // to keep track of pages
        Replacer<Page *, hash_for_Page_pointer> *replacer_;   // to find an unpinned page for replacement
        sjtu::list<Page *> *free_list_; // to find a free page for replacement
    };
} // namespace sjtu
