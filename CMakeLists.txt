cmake_minimum_required(VERSION 2.8)

include_directories(BEFORE ${CMAKE_SOURCE_DIR}/utils)
include_directories(BEFORE ${CMAKE_SOURCE_DIR}/bench)
include_directories(BEFORE ${CMAKE_SOURCE_DIR}/include)
include_directories(BEFORE ${CMAKE_SOURCE_DIR}/wrappers)
include_directories(BEFORE ${CMAKE_INCLUDE_PATH})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GNU_SOURCE -D__DEBUG")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -L${CMAKE_LIBRARY_PATH} -std=c++11")

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel." FORCE)
endif(NOT CMAKE_BUILD_TYPE)

if (NOT WIN32)
    set(PTHREAD_LIB pthread)
    set(LIBM m)
    set(LIBZ z)
    set(LIBSNAPPY snappy)
    set(LIBBZ2 bz2)
    set(LIBFDB forestdb)
    set(LIBLDB leveldb)
    set(LIBRDB rocksdb)
    set(LIBCOUCH couchstore)
    set(LIBWT wiredtiger)
endif(NOT WIN32)

if ("${CMAKE_C_COMPILER_ID}" STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Qunused-arguments -g -fomit-frame-pointer -pthread")
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fomit-frame-pointer -pthread")
    set(LIBRT rt)
endif()

add_executable(fdb_bench
               bench/couch_bench.cc
               wrappers/couch_fdb.cc
               utils/avltree.cc
               utils/stopwatch.cc
               utils/iniparser.cc
               utils/crc32.cc
               utils/memleak.cc
               utils/zipfian_random.cc
               utils/keyloader.cc
               utils/keygen.cc)
target_link_libraries(fdb_bench ${PTHREAD_LIB} ${LIBM} ${LIBSNAPPY} ${LIBFDB})
set_target_properties(fdb_bench PROPERTIES COMPILE_FLAGS "-D__FDB_BENCH")
file(COPY ${CMAKE_SOURCE_DIR}/bench_config.ini DESTINATION ./)

add_executable(couch_bench
               bench/couch_bench.cc
               utils/avltree.cc
               utils/stopwatch.cc
               utils/iniparser.cc
               utils/crc32.cc
               utils/memleak.cc
               utils/zipfian_random.cc
               utils/keyloader.cc
               utils/keygen.cc)
target_link_libraries(couch_bench ${PTHREAD_LIB} ${LIBM} ${LIBSNAPPY} ${LIBCOUCH})
set_target_properties(couch_bench PROPERTIES COMPILE_FLAGS "-D__COUCH_BENCH")
file(COPY ${CMAKE_SOURCE_DIR}/bench_config.ini DESTINATION ./)

add_executable(leveldb_bench
               bench/couch_bench.cc
               wrappers/couch_leveldb.cc
               utils/avltree.cc
               utils/stopwatch.cc
               utils/iniparser.cc
               utils/crc32.cc
               utils/memleak.cc
               utils/zipfian_random.cc
               utils/keyloader.cc
               utils/keygen.cc)
target_link_libraries(leveldb_bench ${PTHREAD_LIB} ${LIBM} ${LIBSNAPPY} ${LIBLDB})
set_target_properties(leveldb_bench PROPERTIES COMPILE_FLAGS "-D__LEVEL_BENCH")
file(COPY ${CMAKE_SOURCE_DIR}/bench_config.ini DESTINATION ./)

add_executable(wt_bench
               bench/couch_bench.cc
               wrappers/couch_wt.cc
               utils/avltree.cc
               utils/stopwatch.cc
               utils/iniparser.cc
               utils/crc32.cc
               utils/memleak.cc
               utils/zipfian_random.cc
               utils/keyloader.cc
               utils/keygen.cc)
target_link_libraries(wt_bench ${PTHREAD_LIB} ${LIBM} ${LIBSNAPPY} ${LIBWT})
set_target_properties(wt_bench PROPERTIES COMPILE_FLAGS "-D__WT_BENCH")
file(COPY ${CMAKE_SOURCE_DIR}/bench_config.ini DESTINATION ./)

add_executable(rocksdb_bench
               bench/couch_bench.cc
               wrappers/couch_rocksdb.cc
               utils/avltree.cc
               utils/stopwatch.cc
               utils/iniparser.cc
               utils/crc32.cc
               utils/memleak.cc
               utils/zipfian_random.cc
               utils/keyloader.cc
               utils/keygen.cc)
target_link_libraries(rocksdb_bench 
                      ${PTHREAD_LIB} ${LIBM} ${LIBSNAPPY} ${LIBRT} ${LIBZ} ${LIBBZ2} ${LIBRDB})
set_target_properties(rocksdb_bench PROPERTIES COMPILE_FLAGS "-D__ROCKS_BENCH")
file(COPY ${CMAKE_SOURCE_DIR}/bench_config.ini DESTINATION ./)
