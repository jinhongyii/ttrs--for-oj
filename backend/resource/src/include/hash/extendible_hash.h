/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once
//#include <list>
#include "list.hpp"
#include <cstdlib>
#include <vector>
#include <string>
#include <unordered_map>
#include "hash/hash_table.h"

namespace sjtu {

template <typename K, typename V, typename hash>
class ExtendibleHash : public HashTable<K, V, hash> {
private:
    struct node {
        K k;
        V v;
        node *nxt;
        node(const K &_a, const V &_b, node *_nxt = nullptr) : k(_a), v(_b), nxt(_nxt) {}
    };

public:
  // constructor
  ExtendibleHash(size_t size);
  // deconstructor
  ~ExtendibleHash();
  void release(node *);
  // helper function to generate hash addressing
  size_t HashKey(const K &key) const;
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;
  int size() override ;
  void clear() override ;
//  std::pair<K , V> pop_back ();
private:
    static const int p = 2003;
    node **hashmap;
    int map_size;
//    std::unordered_map<K, V> hashmap;
//  std::list<std::pair<K,V>> keyvals;
};
} // namespace sjtu
