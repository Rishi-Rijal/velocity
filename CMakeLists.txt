cmake_minimum_required(VERSION 3.20)
project(velocity)

set(CMAKE_CXX_STANDARD 23)

find_package(absl REQUIRED)

add_executable(velocity main.cpp)

target_link_libraries(velocity
        PRIVATE
        absl::flat_hash_map
)