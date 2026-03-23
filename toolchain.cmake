set(CMAKE_SYSTEM_NAME Generic)
set(CMAKE_SYSTEM_PROCESSOR wasm32)

# Point to your LLVM clang++ and wasm-ld (adjust paths if necessary)
set(CMAKE_C_COMPILER clang)
set(CMAKE_CXX_COMPILER clang++)
set(CMAKE_AR llvm-ar)
set(CMAKE_LINKER wasm-ld)

set(CMAKE_CXX_FLAGS "--target=wasm32 -nostdlib -Wl,--no-entry" CACHE STRING "" FORCE)
set(CMAKE_EXE_LINKER_FLAGS "--target=wasm32 -nostdlib -Wl,--no-entry" CACHE STRING "" FORCE)

set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)

