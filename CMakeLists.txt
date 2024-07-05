cmake_minimum_required(VERSION 3.29)
project(pulse-db)

# Setting options for the project.
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Default build type to Release if not given.
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release")
endif()

# Include directory.
include_directories(include)

# Getting sources and headers.
file(GLOB_RECURSE SOURCES src/*.cpp)
file(GLOB_RECURSE HEADERS include/*.hpp include/*.h)

# Option to build tests.
option(BUILD_TESTS "Build tests" OFF)
if (BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

# Adding the library and the executable.
add_library(PulseLib ${SOURCES} ${HEADERS})
add_executable(${PROJECT_NAME} src/main.cpp)
target_link_libraries(${PROJECT_NAME} PulseLib)
