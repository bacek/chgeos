cmake_minimum_required(VERSION 3.10)
project(chgeos LANGUAGES CXX)

set(CMAKE_VERBOSE_MAKEFILE)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Use WASM-native C++ exception handling (new exnref proposal, try_table opcodes).
# Must appear before add_subdirectory calls so GEOS and all other dependencies
# are compiled with the same exception ABI.
#
# -fwasm-exceptions: emit WASM exception opcodes instead of Emscripten's
#   JS-based invoke_* wrappers (which ClickHouse's WasmTime runtime doesn't
#   provide as host imports).
# -sWASM_EXNREF=1: use new-style exnref opcodes (try_table) instead of legacy
#   try/catch opcodes. Required for wasmtime configured with wasm_exceptions(true).
#   Emscripten auto-exports __cpp_exception tag from libcxxabi; this is stripped
#   by strip_tag_exports.py (POST_BUILD below) because wasmtime's C API panics on
#   ExternType::Tag (issue #10252) — no linker flag can suppress the attribute.
if(CMAKE_CROSSCOMPILING)
  add_compile_options(-fexceptions -fwasm-exceptions -sWASM_LEGACY_EXCEPTIONS=0)
  add_link_options(-fexceptions -fwasm-exceptions -sWASM_LEGACY_EXCEPTIONS=0 -sNO_FILESYSTEM=1 -sALLOW_MEMORY_GROWTH=1 -sMAXIMUM_MEMORY=4294967296)
endif()

add_compile_options($<IF:$<CONFIG:Debug>,-O0,-O2>)
add_link_options($<IF:$<CONFIG:Debug>,-O0,-O2>)

add_subdirectory(third-party/geos geos)
if(CMAKE_CROSSCOMPILING)
  set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
else()
  option(BUILD_TESTING "..." ON)
endif()
option(BUILD_EXAMPLES "..." OFF)
add_subdirectory(third-party/msgpack23 msgpack23)

add_executable(chgeos src/main.cpp src/geom/wkb.cpp src/mem.cpp)

if(NOT CMAKE_CROSSCOMPILING)
  enable_testing()
  add_subdirectory(tests)
  # Suppress the NOT_BUILT placeholder test that msgpack23 registers
  configure_file(
    ${CMAKE_SOURCE_DIR}/CTestCustom.cmake.in
    ${CMAKE_BINARY_DIR}/CTestCustom.cmake
    COPYONLY
  )
endif()


target_link_libraries(chgeos PRIVATE geos)
target_link_libraries(chgeos PRIVATE msgpack23)
target_include_directories(chgeos PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/geos/capi)

if(CMAKE_CROSSCOMPILING)
  # Set properties to produce .wasm and configure linker flags
  set_target_properties(chgeos PROPERTIES SUFFIX ".wasm")
  target_link_options(chgeos PRIVATE
    "-Wl,--no-entry"    # Required if no main()
    # "-Wl,--export-all"  # Export all functions
    "-Wl,--allow-undefined" # Allows linker to skip missing system libs
    "-Wl,--allow-multiple-definition" # emscripten 5.0.4+ also defines __throw_exception_with_stack_trace in libstandalonewasm; ours (earlier in link order) wins
  )
endif()
