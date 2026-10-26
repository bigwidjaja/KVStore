cmake_minimum_required(VERSION 3.25)
project(kvstore LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "kvstore needs Linux (epoll, io_uring). On a Mac, build in tools/dev/shell.sh.")
endif()

set(KV_SANITIZE "" CACHE STRING "Sanitizers: 'address' (ASan + UBSan), 'thread' (TSan), or empty")
option(KV_BUILD_TESTS "Build unit tests" ON)
option(KV_BUILD_BENCH "Build microbenchmarks" ON)
option(KV_BUILD_FUZZERS "Build libFuzzer targets (Clang only)" OFF)

add_compile_options(-Wall -Wextra -Wno-unused-parameter -Wno-missing-field-initializers)

if(KV_SANITIZE STREQUAL "address")
  add_compile_options(-fsanitize=address,undefined -fno-sanitize-recover=undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
elseif(KV_SANITIZE STREQUAL "thread")
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
elseif(NOT KV_SANITIZE STREQUAL "")
  message(FATAL_ERROR "Unknown KV_SANITIZE value: ${KV_SANITIZE}")
endif()

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(URING REQUIRED IMPORTED_TARGET liburing>=2.5)
find_package(absl REQUIRED)
find_path(XXHASH_INCLUDE_DIR xxhash.h REQUIRED)

# Everything except main(), so tests and benchmarks can link it.
add_library(kv STATIC
  src/base/log.cpp
  src/base/strings.cpp
  src/proto/resp_parser.cpp
  src/proto/reply_builder.cpp
  src/core/compact_string.cpp
  src/core/dash_table.cpp
  src/core/db_slice.cpp
  src/shard/router.cpp
  src/net/reactor.cpp
  src/net/socket_util.cpp
  src/net/epoll_reactor.cpp
  src/net/uring_reactor.cpp
  src/server/command_table.cpp
  src/server/config.cpp
  src/server/connection.cpp
  src/server/generic_family.cpp
  src/server/server.cpp
  src/server/server_family.cpp
  src/server/string_family.cpp
  src/server/worker.cpp
)
target_include_directories(kv PUBLIC src ${XXHASH_INCLUDE_DIR})
target_link_libraries(kv PUBLIC
  PkgConfig::URING
  absl::flat_hash_map
  absl::inlined_vector
  Threads::Threads
)

add_executable(kv_server src/server/main.cpp)
target_link_libraries(kv_server PRIVATE kv)

if(KV_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(KV_BUILD_BENCH)
  add_subdirectory(bench)
endif()
