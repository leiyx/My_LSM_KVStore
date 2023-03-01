
add_executable(correctness correctness.cc)
target_link_libraries(correctness lsmstore)
add_executable(test_cache test_cache.cc)
target_link_libraries(test_cache lsmstore)
add_executable(test_alloc test_alloc.cc)
target_link_libraries(test_alloc lsmstore)