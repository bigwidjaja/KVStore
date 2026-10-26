find_package(GTest REQUIRED)
include(GoogleTest)

function(kv_unit_test name)
  add_executable(${name} unit/${name}.cpp)
  target_link_libraries(${name} PRIVATE kv GTest::gtest GTest::gtest_main)
  gtest_discover_tests(${name} DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 1200 LABELS unit)
endfunction()

kv_unit_test(strings_test)
