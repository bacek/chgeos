if(NOT TARGET gtest)
  add_subdirectory(${CMAKE_SOURCE_DIR}/third-party/googletest googletest)
endif()

add_executable(chgeos_tests
  test_functions.cpp
  test_mem.cpp
  test_unpack.cpp
  test_bbox_wrapper.cpp
  test_accessors.cpp
  test_constructors.cpp
  test_io.cpp
  test_overlay.cpp
  test_predicates.cpp
  test_processing.cpp
  test_transforms.cpp
  ${CMAKE_SOURCE_DIR}/src/geom/wkb.cpp
  ${CMAKE_SOURCE_DIR}/src/mem.cpp
)

target_include_directories(chgeos_tests PRIVATE
  ${CMAKE_SOURCE_DIR}/src
  ${CMAKE_BINARY_DIR}/geos/capi
)

target_link_libraries(chgeos_tests PRIVATE
  geos
  msgpack23
  GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(chgeos_tests)

# Exclude stray test targets registered by third-party subdirectories
if(TEST msgpack23_tests_NOT_BUILT)
  set_tests_properties(msgpack23_tests_NOT_BUILT PROPERTIES DISABLED TRUE)
endif()
