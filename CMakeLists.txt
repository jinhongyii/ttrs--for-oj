cmake_minimum_required(VERSION 3.5.1)
project(TTRS-2)

set(CMAKE_CXX_STANDARD 14)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3  ")
#set(INC_DIR /mnt/c/Users/jinho/ClionProjects/crypto++/cryptopp810)
#set(LIB_DIR /mnt/c/Users/jinho/ClionProjects/crypto++/cryptopp810)
#include_directories(${INC_DIR})
#link_directories(${LIB_DIR})
#link_libraries(libcryptopp.so)
include_directories(backend/resource/src/include)
#add_executable(bptreetest backend/util/MemoryController.cpp
#        backend/util/bptreetest.cpp
#        backend/console-application/TicketManager.cpp
#        backend/resource/src/buffer/buffer_pool_manager.cpp
#        backend/resource/src/buffer/lru_replacer.cpp
#        backend/resource/src/hash/extendible_hash.cpp
#        backend/resource/src/disk/disk_manager.cpp
#        backend/resource/src/hash/hash_method.cpp
#        backend/resource/src/page/header_page.cpp
#        backend/resource/src/page/table_page.cpp
#        backend/resource/src/page/directory_page.hpp
#        backend/resource/src/table/new_table_heap.cpp
#        backend/resource/src/table/tuple.cpp
#        backend/resource/src/common/config.cpp
#        backend/resource/src/logging/log_manager.cpp
#        backend/resource/src/concurrency/transaction_manager.cpp
#        backend/util.cpp)
#target_link_libraries(TTRS__backend libcryptopp.so)
#add_executable(stringtest backend/util/stringtest.cpp)
include_directories(backend/util)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__VTableFILE__='\"$(subst ${CMAKE_SOURCE_DIR}/,,$(abspath $<))\"'")

add_executable(code
        backend/console-application/Console.cpp
        backend/console-application/TrainManager.cpp
        backend/console-application/TicketQuery.cpp
        backend/console-application/TicketManager.cpp
        backend/resource/src/buffer/buffer_pool_manager.cpp
        backend/resource/src/buffer/lru_replacer.cpp
        backend/resource/src/hash/extendible_hash.cpp
        backend/resource/src/disk/disk_manager.cpp
        backend/resource/src/hash/hash_method.cpp
        backend/resource/src/page/header_page.cpp
        backend/resource/src/page/table_page.cpp
        backend/resource/src/page/directory_page.hpp
        backend/resource/src/table/new_table_heap.cpp
        backend/resource/src/table/tuple.cpp
        backend/resource/src/common/config.cpp
        backend/resource/src/logging/log_manager.cpp
        backend/resource/src/concurrency/transaction_manager.cpp
        backend/util.cpp
        )

#add_executable(Test
#        backend/util/TestBulkLoading.cpp
#        )

#add_subdirectory(backend/resource)
#
#add_executable(classTester backend/console-application/TrainManager.cpp backend/console-application/TicketQuery.cpp
#        backend/console-application/classTester.cpp
#        backend/util/MemoryController.cpp
#        backend/resource/src/buffer/buffer_pool_manager.cpp
#        backend/resource/src/buffer/lru_replacer.cpp
#        backend/resource/src/hash/extendible_hash.cpp
#        backend/resource/src/disk/disk_manager.cpp
#        backend/resource/src/hash/hash_method.cpp
#        backend/resource/src/page/header_page.cpp
#        backend/resource/src/page/table_page.cpp
#        backend/resource/src/table/table_heap.cpp
#        backend/resource/src/table/tuple.cpp
#        backend/resource/src/common/config.cpp
#        backend/resource/src/logging/log_manager.cpp
#        backend/resource/src/concurrency/transaction_manager.cpp
#        backend/resource/src/type/type.cpp
#        backend/resource/src/type/value.cpp
#        backend/resource/src/type/bigint_type.cpp
#        backend/resource/src/type/boolean_type.cpp
#        backend/resource/src/type/integer_type.cpp
#        backend/resource/src/type/varlen_type.cpp
#        backend/resource/src/type/decimal_type.cpp
#        backend/resource/src/type/tinyint_type.cpp
#        backend/resource/src/type/smallint_type.cpp
#        backend/resource/src/type/integer_parent_type.cpp
#        backend/util.cpp)
#

#find_package(Boost REQUIRED COMPONENTS
#        system)
#add_executable(server
#        backend/console-application/server.cpp
#        backend/console-application/user.cpp
#        backend/console-application/TrainManager.cpp
#        backend/console-application/TicketQuery.cpp
#        backend/util/MemoryController.cpp
#        backend/console-application/TicketManager.cpp
#        )
#target_link_libraries(server ${Boost_LIBRARIES})

