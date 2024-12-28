include_directories(${CMAKE_SOURCE_DIR}/include)
project(pulsedb-tests)

# Find Catch2.
find_package(Catch2 3 REQUIRED)
message(STATUS "Catch2 found: ${Catch2_VERSION}")

# Set test files.
file(GLOB_RECURSE TEST_FILES pulsedb/*.cpp )

# Setup catch2.
add_executable(${PROJECT_NAME} ${TEST_FILES})
target_link_libraries(${PROJECT_NAME} 
    PRIVATE 
    Catch2::Catch2WithMain
    PulseLib
)
