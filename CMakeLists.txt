cmake_minimum_required(VERSION 3.25) # CMake 3.25+ natively supports C++26
project(Cpp26Sandbox LANGUAGES CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Enforce C++26 standard
set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF) # Ensures clean, standard-compliant code

# Find the system's standard thread library (pthread on Mac/Linux, internal on Windows)
find_package(Threads REQUIRED)

# Define the executable target
add_executable(sandbox 
    test.cpp 
    dbms.cpp
)

# Link the threading library (Applies to all operating systems)
target_link_libraries(sandbox PRIVATE Threads::Threads)

# Link Winsock only if compiling on Windows
if(WIN32)
    target_link_libraries(sandbox PRIVATE ws2_32)
endif()