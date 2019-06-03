/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include "buffer/replacer.h"
#include "hash/extendible_hash.h"
#include "page/page.h"
namespace sjtu {

template <typename T, typename hash_for_T> class LRUReplacer : public Replacer<T, hash_for_T> {
public:
  // do not change public interface
  LRUReplacer (int maxsz);

  ~LRUReplacer();

  void Insert(const T &value);

  bool Victim(T &value);

  bool Erase(const T &value);

  size_t Size();

  void clear();

private:
    ExtendibleHash<T,typename sjtu::list<T>::iterator, hash_for_T> hashtable;
    sjtu::list<T> vals;
    int maxsize;

};

} // namespace sjtu
