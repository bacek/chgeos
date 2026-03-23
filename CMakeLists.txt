cmake_minimum_required(VERSION 3.10)
project(chgeos LANGUAGES CXX)

# Define the target wasm file
add_executable(chgeos main.cpp)

# Set properties to produce .wasm and configure linker flags
set_target_properties(chgeos PROPERTIES SUFFIX ".wasm")
target_link_options(chgeos PRIVATE 
    "-Wl,--no-entry"    # Required if no main()
    "-Wl,--export-all"  # Export all functions
    "-Wl,--allow-undefined" # Allows linker to skip missing system libs
)

