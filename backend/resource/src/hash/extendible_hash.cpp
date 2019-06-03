
#include "hash/extendible_hash.h"
#include "page/page.h"
#include "buffer/buffer_pool_manager.h"
#include "hash/hash_method.h"
//#include <cstring>

namespace sjtu {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V, typename hash>
ExtendibleHash<K, V, hash>::ExtendibleHash(size_t size) {
    hashmap = new node* [p];
    memset(hashmap, 0, sizeof(node *) * p);
    map_size = 0;
}

template<typename K, typename V, typename hash>
ExtendibleHash<K, V, hash>::~ExtendibleHash() {
    for (int i = 0; i < p; ++i)
        release(hashmap[i]);
    delete [] hashmap;
}

template<typename K, typename V, typename hash>
void ExtendibleHash<K, V, hash>::release(ExtendibleHash::node *t) {
    if (t == nullptr) return;
    release(t->nxt);
    delete(t);
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V, typename hash>
size_t ExtendibleHash<K, V, hash>::HashKey(const K &key) const {
    return hash()(key);
//  return (size_t )(key)%hashmap.bucket_count();
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V, typename hash>
int ExtendibleHash<K, V, hash>::GetGlobalDepth() const {
    return p;
//  return hashmap.max_bucket_count();
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V, typename hash>
int ExtendibleHash<K, V, hash>::GetLocalDepth(int bucket_id) const {
    return p;
//  return hashmap.bucket_size(bucket_id);
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V, typename hash>
int ExtendibleHash<K, V, hash>::GetNumBuckets() const {
    return p;
//  return hashmap.bucket_count();
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V, typename hash>
bool ExtendibleHash<K, V, hash>::Find(const K &key, V &value) {
    int index = HashKey(key);
    node *t = hashmap[index];
    while (t != nullptr) {
        if (t->k == key) {
            value = t->v;
            return true;
        }
        t = t->nxt;
    }
    return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V, typename hash>
bool ExtendibleHash<K, V, hash>::Remove(const K &key) {
    int index = HashKey(key);
    node *t = hashmap[index], *pre = nullptr;
    while (t != nullptr) {
        if (t->k == key) {
            if (pre == nullptr) {
                hashmap[index] = t->nxt;
                delete t;
            } else {
                pre->nxt = t->nxt;
                delete t;
            }
            --map_size;
            return true;
        }
        pre = t;
        t = t->nxt;
    }
    return false;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V, typename hash>
void ExtendibleHash<K, V, hash>::Insert(const K &key, const V &value) {
//        hashmap[key]=value;
    int index = HashKey(key);
    ++map_size;
    hashmap[index] = new node(key, value, hashmap[index]);
}

    template<typename K , typename V, typename hash>
    int ExtendibleHash<K , V, hash>::size () {
        return map_size;
    }

    template<typename K , typename V, typename hash>
    void ExtendibleHash<K , V, hash>::clear () {
        for (int i = 0; i < p; ++i) release(hashmap[i]), hashmap[i] = nullptr;
        map_size = 0;
    }

//    template<typename K , typename V>
//    std::pair<K , V> ExtendibleHash<K , V>::pop_back () {
//        auto tmp=keyvals.back();
//        keyvals.pop_back();
//        hashmap.erase(tmp.first);
//        return tmp;
//    }

template class ExtendibleHash<page_id_t, Page *, hash_for_int>;
template class ExtendibleHash<Page *, sjtu::list<Page *>::iterator, hash_for_Page_pointer>;
// test purpose
template class ExtendibleHash<int, std::string, hash_for_int>;
template class ExtendibleHash<int, sjtu::list<int>::iterator, hash_for_int>;
template class ExtendibleHash<int, int, hash_for_int>;
template class ExtendibleHash<Page *, int, hash_for_Page_pointer>;
} // namespace sjtu
