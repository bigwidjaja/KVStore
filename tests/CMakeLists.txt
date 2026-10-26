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

if(KV_BUILD_FUZZERS)
  add_executable(resp_parser_fuzz fuzz/resp_parser_fuzz.cpp)
  target_link_libraries(resp_parser_fuzz PRIVATE kv)
  target_compile_options(resp_parser_fuzz PRIVATE -fsanitize=fuzzer)
  target_link_options(resp_parser_fuzz PRIVATE -fsanitize=fuzzer)
endif()
