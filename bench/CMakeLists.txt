find_package(benchmark REQUIRED)

# Google Benchmark: insert/lookup throughput of our table vs std::unordered_map and
# absl::flat_hash_map (Phase 3, step 9), and queue throughput (Phase 4, step 5).
add_executable(table_bench micro/table_bench.cpp)
target_link_libraries(table_bench PRIVATE kv benchmark::benchmark absl::flat_hash_map)

add_executable(queue_bench micro/queue_bench.cpp)
target_link_libraries(queue_bench PRIVATE kv benchmark::benchmark)

# Plain programs that write CSVs for bench/results/phase3.
add_executable(growth_latency micro/growth_latency.cpp)
target_link_libraries(growth_latency PRIVATE kv absl::flat_hash_map)

add_executable(bytes_per_key micro/bytes_per_key.cpp)
target_link_libraries(bytes_per_key PRIVATE kv absl::flat_hash_map)
