//
// Created by abclzr on 2019/5/22.
//

#ifndef TTRS_2_BULKLOADING_H
#define TTRS_2_BULKLOADING_H

#include <functional>
#include <cstring>
#include "utility.hpp"
#include "string.hpp"
#include "file_manager.h"
#include <iostream>

#define SAVE_FILE_SPACE true

using sjtu::pair;
using std::fstream;
using std::ios;
using std::swap;

namespace sjtu {
    typedef sjtu::string<50> String;

    template<class Key, class Value, class Compare = std::less<Key>>
    class BulkLoading {

    public:
        explicit BulkLoading(const String &);

        ~BulkLoading();

        void append(const Key &, const Value &);

        void clear();

        // once sort the Bulk, you can never append.
        void sort();

        // Initialize the scanning, return the total number of pairs.
        int ready_to_get_next();
        bool get_next(pair<Key, Value> &);

        void save();

    private:
        void stable_sort(pair<Key, Value> *, int, int);

        void flush_all();

        String FILENAME;
        //the number of pairs in the file
        int total=0;
        off_t offset;

        static const int PAIR_SIZE = sizeof(Key) + sizeof(Value);

        //the size of used area of buffer
        int pos;
        char *buffer;

        int next_id;

        fstream BulkFile;
        file_manager<Key, Value, Compare> *bul;
    };

    template<class Key, class Value, class Compare>
    BulkLoading<Key, Value, Compare>::BulkLoading(const String &st) : FILENAME(st) {
        String filename(FILENAME + String("_BulkRecord"));
        BulkFile.open(filename, ios::in | ios::out | ios::binary);
        if (!BulkFile.is_open()) {
            BulkFile.open(filename, ios::out | ios::binary);
            int num = 0;
            BulkFile.write(reinterpret_cast<char *>(&num), sizeof(num));
            BulkFile.close();
            BulkFile.open(filename, ios::in | ios::out | ios::binary);
        }

        BulkFile.read(reinterpret_cast<char *>(&total), sizeof(int));
        offset = sizeof(int) + PAIR_SIZE * total;

        buffer = new char[BUFFER_SIZE];
        pos = 0;
        next_id = 1;
    }

    template<class Key, class Value, class Compare>
    BulkLoading<Key, Value, Compare>::~BulkLoading() {
        save();
        if (buffer != nullptr) delete [] buffer;
        if (bul != nullptr) delete bul;
        BulkFile.close();
//        clear();
    }

    template<class Key, class Value, class Compare>
    void BulkLoading<Key, Value, Compare>::append(const Key &key, const Value &value) {
        if (pos + sizeof(PAIR_SIZE) > BUFFER_SIZE)
            flush_all();
        memcpy(buffer + pos, &key, sizeof(key));
        pos += sizeof(key);
        memcpy(buffer + pos, &value, sizeof(value));
        pos += sizeof(value);
    }

    template<class Key, class Value, class Compare>
    void BulkLoading<Key, Value, Compare>::flush_all() {
        BulkFile.seekp(offset);
        BulkFile.write(reinterpret_cast<char *>(buffer), pos);
        offset += pos;
        total += pos / PAIR_SIZE;
        pos = 0;
    }

    template<class Key, class Value, class Compare>
    void BulkLoading<Key, Value, Compare>::sort() {
        save();
        if (false && buffer != nullptr) {
            delete [] buffer;
            buffer = nullptr;
        }
        BulkFile.close();
        file_manager<Key, Value, Compare> ori(FILENAME + String("_BulkRecord")), file1(FILENAME + String("_sort_file1")), file2(FILENAME + String("_sort_file2"));
        file_manager<Key, Value, Compare> *from = &file1, *to = &file2;
        int nn = (total - 1) / (BUFFER_SIZE / PAIR_SIZE) + 1;
        from->seekp(0);
        from->write(reinterpret_cast<char *>(&nn), sizeof(nn));
        int tot = BUFFER_SIZE / PAIR_SIZE;
        pair<Key, Value> a[tot];
        for (int i = 0; i < total; ++i) {
            ori.seekg(sizeof(int) + PAIR_SIZE * i);
            int n = 0;
            while (i + n < total && n < tot) {
                ori.read(reinterpret_cast<char *>(&a[n].first), sizeof(Key));
                ori.read(reinterpret_cast<char *>(&a[n].second), sizeof(Value));
                ++n;
            }
            i += n - 1;
            stable_sort(a, 0, n - 1);

            from->write(reinterpret_cast<char *>(&n), sizeof(n));
            for (int j = 0; j < n; ++j) {
                from->write(reinterpret_cast<char *>(&a[j].first), sizeof(Key));
                from->write(reinterpret_cast<char *>(&a[j].second), sizeof(Value));
            }
        }

        Compare cmp;
        int m, m1, m2, addm;
        while (true) {
            from->seekg(0);
            from->read(reinterpret_cast<char *>(&m), sizeof(m));
            if (m == 1) break;

            to->seekp(0);
            int md2 = (m + 1) / 2;
            to->write(reinterpret_cast<char *>(&md2), sizeof(m));

            off_t nowtmp = sizeof(int);
            pair<Key, Value> p1, p2;
            for (int i = 1; i <= m; i += 2) {
                if (i == m) {
                    from->seekg(nowtmp);
                    from->read(reinterpret_cast<char *>(&m1), sizeof(m1));
                    to->write(reinterpret_cast<char *>(&m1), sizeof(m1));
                    for (int j = 1; j <= m1; ++j) {
                        from->read(reinterpret_cast<char *>(&p1.first), sizeof(p1.first));
                        from->read(reinterpret_cast<char *>(&p1.second), sizeof(p1.second));
                        to->write(reinterpret_cast<char *>(&p1.first), sizeof(p1.first));
                        to->write(reinterpret_cast<char *>(&p1.second), sizeof(p1.second));
                    }
                    break;
                }
                from->seekg(nowtmp);
                from->read(reinterpret_cast<char *>(&m1), sizeof(m1));
                nowtmp += sizeof(int) + PAIR_SIZE * m1;
                from->seekg2(nowtmp);
                from->read2(reinterpret_cast<char *>(&m2), sizeof(m2));
                nowtmp += sizeof(int) + PAIR_SIZE * m2;
                addm = m1 + m2;
                to->write(reinterpret_cast<char *>(&addm), sizeof(addm));

                int t1 = 1, t2 = 1;
                from->read(reinterpret_cast<char *>(&p1.first), sizeof(p1.first));
                from->read(reinterpret_cast<char *>(&p1.second), sizeof(p1.second));
                from->read2(reinterpret_cast<char *>(&p2.first), sizeof(p2.first));
                from->read2(reinterpret_cast<char *>(&p2.second), sizeof(p2.second));
                while (t1 <= m1 && t2 <= m2) {
                    if (cmp(p1.first, p2.first)) {
                        to->write(reinterpret_cast<char *>(&p1.first), sizeof(p1.first));
                        to->write(reinterpret_cast<char *>(&p1.second), sizeof(p1.second));
                        ++t1;
                        if (t1 > m1) break;
                        from->read(reinterpret_cast<char *>(&p1.first), sizeof(p1.first));
                        from->read(reinterpret_cast<char *>(&p1.second), sizeof(p1.second));
                    } else {
                        to->write(reinterpret_cast<char *>(&p2.first), sizeof(p2.first));
                        to->write(reinterpret_cast<char *>(&p2.second), sizeof(p2.second));
                        ++t2;
                        if (t2 > m2) break;
                        from->read2(reinterpret_cast<char *>(&p2.first), sizeof(p2.first));
                        from->read2(reinterpret_cast<char *>(&p2.second), sizeof(p2.second));
                    }
                }
                while (t1 <= m1) {
                    to->write(reinterpret_cast<char *>(&p1.first), sizeof(p1.first));
                    to->write(reinterpret_cast<char *>(&p1.second), sizeof(p1.second));
                    ++t1;
                    if (t1 > m1) break;
                    from->read(reinterpret_cast<char *>(&p1.first), sizeof(p1.first));
                    from->read(reinterpret_cast<char *>(&p1.second), sizeof(p1.second));
                }
                while (t2 <= m2) {
                    to->write(reinterpret_cast<char *>(&p2.first), sizeof(p2.first));
                    to->write(reinterpret_cast<char *>(&p2.second), sizeof(p2.second));
                    ++t2;
                    if (t2 > m2) break;
                    from->read2(reinterpret_cast<char *>(&p2.first), sizeof(p2.first));
                    from->read2(reinterpret_cast<char *>(&p2.second), sizeof(p2.second));
                }
            }
            swap(from, to);
        }

        ori.close();
        ori.open_out(FILENAME + String("_BulkRecord"));
        ori.seekp(0);
        from->seekg(sizeof(int));
        from->read(reinterpret_cast<char *>(&m), sizeof(m));
        ori.write(reinterpret_cast<char *>(&m), sizeof(m));
        pair<Key, Value> p;
        for (int i = 1; i <= m; ++i) {
            from->read(reinterpret_cast<char *>(&p.first), sizeof(p.first));
            from->read(reinterpret_cast<char *>(&p.second), sizeof(p.second));
            ori.write(reinterpret_cast<char *>(&p.first), sizeof(p.first));
            ori.write(reinterpret_cast<char *>(&p.second), sizeof(p.second));
        }

        from->close();
        to->close();
        ori.close();
        if (SAVE_FILE_SPACE) {
            from->clean(FILENAME + String("_sort_file1"));
            from->clean(FILENAME + String("_sort_file2"));
        }
    }

    template<class Key, class Value, class Compare>
    void BulkLoading<Key, Value, Compare>::save() {
        flush_all();
        BulkFile.seekp(0);
        BulkFile.write(reinterpret_cast<char *>(&total), sizeof(total));
    }

    // Actually it's a quick sort. The merge sort is slow.
    template<class Key, class Value, class Compare>
    void BulkLoading<Key, Value, Compare>::stable_sort(pair<Key, Value> *a, int start, int end) {
        if (start >= end)
            return;
        pair<Key, Value> mid = a[end];
        int left = start, right = end - 1;
        Compare cmp;
        while (left < right) {
            while (cmp(a[left].first, mid.first) && left < right)
                left++;
            while (cmp(mid.first, a[right].first) && left < right)
                right--;
            std::swap(a[left], a[right]);
        }
        if (cmp(a[end].first, a[left].first))
            std::swap(a[left], a[end]);
        else
            left++;
        stable_sort(a, start, left - 1);
        stable_sort(a, left + 1, end);

        /*
        if (l >= r) return;
        int mid = l + r >> 1;
        stable_sort(a, l, mid);
        stable_sort(a, mid + 1, r);
        pair<Key, Value> b[r - l + 1];
        memcpy(b, a + sizeof(pair<Key, Value>) * l, sizeof(pair<Key, Value>) * (r - l + 1));
        int t1 = l, t2 = mid + 1, tmp = l;
        Compare cmp;
        while (t1 <= mid && t2 <= r) {
            if (cmp(b[t1 - l], b[t2 - l])) {
                a[tmp++] = b[t1 - l];
                ++t1;
            }
            else {
                a[tmp++] = b[t2 - l];
                ++t2;
            }
        }
        while (t1 <= mid) a[tmp++] = b[t1 - l], ++t1;
        while (t2 <= r) a[tmp++] = b[t2 - l], ++t2;
        */
    }

    template<class Key, class Value, class Compare>
    int BulkLoading<Key, Value, Compare>::ready_to_get_next() {
        bul = new file_manager<Key, Value, Compare>(FILENAME + String("_BulkRecord"));
        bul->seekg(0);
        bul->read(reinterpret_cast<char *>(&total), sizeof(total));
        return total;
    }

    template<class Key, class Value, class Compare>
    void BulkLoading<Key, Value, Compare>::clear() {
        fstream ff;
        if (!BulkFile.is_open()) {
            ff.open(FILENAME + String("_BulkRecord"), ios::binary | ios::out);
            total = 0;
            ff.write(reinterpret_cast<char *>(&total), sizeof(total));
            ff.close();
        } else {
            BulkFile.close();
            ff.open(FILENAME + String("_BulkRecord"), ios::binary | ios::out);
            total = 0;
            ff.write(reinterpret_cast<char *>(&total), sizeof(total));
            ff.close();
            BulkFile.open(FILENAME + String("_BulkRecord"), ios::binary | ios::in | ios::out);
            pos = 0; next_id = 1;
            BulkFile.read(reinterpret_cast<char *>(&total), sizeof(total));
            offset = sizeof(int);
        }
        ff.open(FILENAME + String("sort_file1"), ios::binary | ios::out);
        ff.close();
        ff.open(FILENAME + String("sort_file2"), ios::binary | ios::out);
        ff.close();
    }

    template<class Key, class Value, class Compare>
    bool BulkLoading<Key, Value, Compare>::get_next(pair<Key, Value> &p) {
        --total;
        bul->read(reinterpret_cast<char *>(&p.first), sizeof(p.first));
        bul->read(reinterpret_cast<char *>(&p.second), sizeof(p.second));
        return total > 0;
    }

}

#endif //TTRS_2_BULKLOADING_H
