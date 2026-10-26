find_package(GTest REQUIRED)
include(GoogleTest)

function(kv_unit_test name)
  add_executable(${name} unit/${name}.cpp)
  target_link_libraries(${name} PRIVATE kv GTest::gtest GTest::gtest_main)
  gtest_discover_tests(${name} DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 1200 LABELS unit)
endfunction()

kv_unit_test(strings_test)
kv_unit_test(resp_parser_test)
kv_unit_test(reply_builder_test)
kv_unit_test(compact_string_test)
kv_unit_test(dash_table_test)
kv_unit_test(db_slice_test)
kv_unit_test(table_diff_test)
kv_unit_test(inline_task_test)
kv_unit_test(mpsc_queue_test)
kv_unit_test(router_test)
kv_unit_test(reactor_test)

# Integration suite: pytest + redis-py against the real binary, once per backend.
find_package(Python3 COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND)
  foreach(backend epoll uring)
    add_test(NAME integration_${backend}
             COMMAND ${Python3_EXECUTABLE} -m pytest -q -p no:cacheprovider
                     ${CMAKE_CURRENT_SOURCE_DIR}/integration)
    set_tests_properties(integration_${backend} PROPERTIES
      ENVIRONMENT "KV_SERVER_BIN=$<TARGET_FILE:kv_server>;KV_BACKEND=${backend}"
      TIMEOUT 3600
      LABELS integration)
  endforeach()
endif()

if(KV_BUILD_FUZZERS)
  add_executable(resp_parser_fuzz fuzz/resp_parser_fuzz.cpp)
  target_link_libraries(resp_parser_fuzz PRIVATE kv)
  target_compile_options(resp_parser_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(resp_parser_fuzz PRIVATE -fsanitize=fuzzer)
endif()
