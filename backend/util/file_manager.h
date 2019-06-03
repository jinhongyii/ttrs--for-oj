//
// Created by abclzr on 2019/5/23.
//

#ifndef TTRS_2_FILE_MANAGER_H
#define TTRS_2_FILE_MANAGER_H

#include <fstream>
#include <functional>
#include "common/config.h"
using std::fstream;
using std::ios;

namespace sjtu {

    typedef sjtu::string<50> String;
// This is a naive file manager, supporting two buffers while reading(used for merge sort).
// Remember that if you need to write, you need to seekp first! Read-operation is the same.
// Don't read immediately after write without a seekg-operation.
    template <class Key, class Value, class Compare = std::less<Key>>
    class file_manager {
    public:
        explicit file_manager(const String &);

        ~file_manager();

        void seekp(off_t);

        void write(char *, off_t);

        void seekg(off_t);

        void seekg2(off_t);

        void read(char *, off_t);

        void read2(char *, off_t);

        void open_out(const String &);

        void close();

        void clean(const String &);

        //ban
        void swap(file_manager &);

        void test();

        void update_filesize(off_t);

    private:

        off_t filesize;
        bool is_writing;
        off_t g, g2, p;
        off_t offset_g, offset_g2, offset_p;
        off_t buffer_size, buffer_size2;
        char *buffer, *buffer2;
        fstream file;
    };

    template<class Key, class Value, class Compare>
    file_manager<Key, Value, Compare>::file_manager(const String &filename) {
        file.open(filename, ios::binary | ios::in | ios::out);
        if (!file.is_open()) {
            file.open(filename, ios::binary | ios::out);
            file.close();
            file.open(filename, ios::binary | ios::in | ios::out);
        }
        file.seekg(0, ios::end);
        filesize = file.tellg();
        buffer = new char [BUFFER_SIZE];
        buffer2 = new char [BUFFER_SIZE];
        buffer_size2 = buffer_size = 0;
        p = g2 = g = 0;
        offset_p = offset_g = offset_g2 = 0;
        is_writing = false;
    }

    template<class Key, class Value, class Compare>
    file_manager<Key, Value, Compare>::~file_manager() {
        if (file.is_open()) file.close();
        delete [] buffer;
        delete [] buffer2;
    }

    template<class Key, class Value, class Compare>
    void file_manager<Key, Value, Compare>::seekp(off_t now_p) {
        if (is_writing && offset_p != p) {
            file.seekp(p);
            file.write(buffer, offset_p - p);
            p = offset_p;
        }
        is_writing = true;
        p = offset_p = now_p;
    }

    template<class Key, class Value, class Compare>
    void file_manager<Key, Value, Compare>::write(char * bu, off_t size) {
        if (offset_p + size - p > BUFFER_SIZE) {
            file.seekp(p);
            file.write(buffer, offset_p - p);
            p = offset_p;
        }
        if (size > BUFFER_SIZE) {
            file.seekp(p);
            file.write(bu, size);
            offset_p += size;
            p = offset_p;
        } else {
            memcpy(buffer + offset_p - p, bu, size);
            offset_p += size;
        }
        update_filesize(offset_p);
    }

    template<class Key, class Value, class Compare>
    void file_manager<Key, Value, Compare>::seekg(off_t now_g) {
        if (is_writing && offset_p != p) {
            file.seekp(p);
            file.write(buffer, offset_p - p);
            p = offset_p;
        }
        is_writing = false;
        g = offset_g = now_g;
        file.seekg(g);
        if (g + BUFFER_SIZE <= filesize) buffer_size = BUFFER_SIZE;
        else buffer_size = filesize - g;
        file.read(buffer, buffer_size);
    }

    template<class Key, class Value, class Compare>
    void file_manager<Key, Value, Compare>::read(char * bu, off_t size) {
        if (offset_g + size - g > buffer_size) {
            g = offset_g;
            file.seekg(g);
            if (g + BUFFER_SIZE <= filesize) buffer_size = BUFFER_SIZE;
            else buffer_size = filesize - g;
            file.read(buffer, buffer_size);
        }
        if (size > BUFFER_SIZE) {
            file.seekg(offset_g);
            file.read(bu, size);
            offset_g += size;
            g = offset_g;
            file.seekg(g);
            if (g + BUFFER_SIZE <= filesize) buffer_size = BUFFER_SIZE;
            else buffer_size = filesize - g;
            file.read(buffer, buffer_size);
        } else {
            memcpy(bu, buffer + offset_g - g, size);
            offset_g += size;
        }
    }

    template<class Key, class Value, class Compare>
    void file_manager<Key, Value, Compare>::seekg2(off_t now_g) {
        if (is_writing && offset_p != p) {
            file.seekp(p);
            file.write(buffer, offset_p - p);
            p = offset_p;
        }
        is_writing = false;
        g2 = offset_g2 = now_g;
        file.seekg(g2);
        if (g2 + BUFFER_SIZE <= filesize) buffer_size2 = BUFFER_SIZE;
        else buffer_size2 = filesize - g2;
        file.read(buffer2, buffer_size2);
    }

    template<class Key, class Value, class Compare>
    void file_manager<Key, Value, Compare>::read2(char * bu, off_t size) {
        if (offset_g2 + size - g2 > buffer_size2) {
            g2 = offset_g2;
            file.seekg(g2);
            if (g2 + BUFFER_SIZE <= filesize) buffer_size2 = BUFFER_SIZE;
            else buffer_size2 = filesize - g2;
            file.read(buffer2, buffer_size2);
        }
        if (size > BUFFER_SIZE) {
            file.seekg(offset_g2);
            file.read(bu, size);
            offset_g2 += size;
            g2 = offset_g2;
            file.seekg(g2);
            if (g2 + BUFFER_SIZE <= filesize) buffer_size2 = BUFFER_SIZE;
            else buffer_size2 = filesize - g2;
            file.read(buffer2, buffer_size2);
        } else {
            memcpy(bu, buffer2 + offset_g2 - g2, size);
            offset_g2 += size;
        }
    }

    template<class Key, class Value, class Compare>
    void file_manager<Key, Value, Compare>::open_out(const String &filename) {
        file.open(filename, ios::binary | ios::out);
        filesize = 0;
    }

    template<class Key, class Value, class Compare>
    void file_manager<Key, Value, Compare>::close() {
        if (is_writing && offset_p > p)
            file.write(buffer, offset_p - p);
        file.close();
    }

    template<class Key, class Value, class Compare>
    void file_manager<Key, Value, Compare>::clean(const String &filename) {
        fstream file2;
        file2.open(filename, ios::binary | ios::out);
        file2.close();
    }

    template<class Key, class Value, class Compare>
    void file_manager<Key, Value, Compare>::swap(file_manager &other) {
        std::swap(is_writing, other.is_writing);
        std::swap(g, other.g);
        std::swap(g2, other.g2);
        std::swap(p, other.p);
        std::swap(offset_g, other.offset_g);
        std::swap(offset_g2, other.offset_g2);
        std::swap(offset_p, other.offset_p);
        std::swap(buffer, other.buffer);
        std::swap(buffer2, other.buffer2);
        file.swap(other.file);
    }

    template<class Key, class Value, class Compare>
    void file_manager<Key, Value, Compare>::test() {
        file.seekg(4);
        std::cout << file.tellg() << std::endl;
    }

    template<class Key, class Value, class Compare>
    void file_manager<Key, Value, Compare>::update_filesize(off_t x) {
        if (x > filesize) filesize = x;
    }

}
#endif //TTRS_2_FILE_MANAGER_H
