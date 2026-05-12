cmake_minimum_required(VERSION 3.10)
project(chgeos LANGUAGES CXX)

set(CMAKE_VERBOSE_MAKEFILE)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()
# Only force -O0 for Debug to avoid overriding Release's -O3.
add_compile_options($<$<CONFIG:Debug>:-O0>)
add_link_options($<$<CONFIG:Debug>:-O0>)

# WASM-native C++ exception handling — must appear before add_subdirectory so GEOS
# and all dependencies are compiled with the same exception ABI.
# -fwasm-exceptions: WASM opcodes instead of Emscripten JS invoke_* wrappers (which
#   ClickHouse's WasmTime runtime doesn't provide as host imports).
# -sWASM_LEGACY_EXCEPTIONS=0: new-style exnref/try_table opcodes required for
#   wasmtime configured with wasm_exceptions(true).
if(CMAKE_CROSSCOMPILING)
  add_compile_options(-fexceptions -fwasm-exceptions -flto -sWASM_LEGACY_EXCEPTIONS=0)
  add_link_options(-fexceptions -fwasm-exceptions -flto -sWASM_LEGACY_EXCEPTIONS=0
                   -sNO_FILESYSTEM=1 -sALLOW_MEMORY_GROWTH=1 -sMAXIMUM_MEMORY=4294901760)
endif()

add_subdirectory(third-party/geos geos)

# Suppress third-party tests; restored below for our own tests (native only).
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
option(BUILD_EXAMPLES "" OFF)
add_subdirectory(third-party/msgpack23 msgpack23)

if(CMAKE_CROSSCOMPILING)
  add_executable(chgeos src/main.cpp src/geom/wkb.cpp src/mem.cpp)
  target_link_libraries(chgeos PRIVATE geos msgpack23)
  target_include_directories(chgeos PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}/geos/capi
    ${CMAKE_CURRENT_SOURCE_DIR}/third-party/xxhash)
  set_target_properties(chgeos PROPERTIES SUFFIX ".wasm")
  target_link_options(chgeos PRIVATE
    "-Wl,--no-entry"
    "-Wl,--allow-undefined"
    # emscripten 5.0.4+ also defines __throw_exception_with_stack_trace in
    # libstandalonewasm; ours (earlier in link order) wins.
    "-Wl,--allow-multiple-definition"
  )
else()
  set(BUILD_TESTING ON CACHE BOOL "" FORCE)
  enable_testing()
  add_subdirectory(tests)
  # Suppress the NOT_BUILT placeholder test that msgpack23 registers.
  configure_file(
    ${CMAKE_SOURCE_DIR}/CTestCustom.cmake.in
    ${CMAKE_BINARY_DIR}/CTestCustom.cmake
    COPYONLY
  )
endif()
