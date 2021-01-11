cmake_minimum_required(VERSION 3.17)
project(SQLCliODBC C CXX)

option(BUILD_PYTHON "Build Python binaries" ON)
set(CMAKE_C_STANDARD 11)
set(CMAKE_SHARED_LIBRARY_PREFIX "")
set(CMAKE_SHARED_LIBRARY_SUFFIX ".pyd")

# Use the first Python installation on PATH, not the newest one
set(Python3_FIND_STRATEGY "LOCATION")
# On Windows, use registry last, not first
set(Python3_FIND_REGISTRY "LAST")
# On macOS, use framework last, not first
set(Python3_FIND_FRAMEWORK "LAST")

find_package(Python3 COMPONENTS Interpreter Development)


aux_source_directory(. SRC_LIST) # 搜索当前目录下的所有.c或者.cpp文件

add_library(${PROJECT_NAME} SHARED ${SRC_LIST})

include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${Python3_INCLUDE_DIRS}
)

target_include_directories(${PROJECT_NAME} PRIVATE ${Python3_INCLUDE_DIRS})
target_link_libraries(${PROJECT_NAME} ${Python3_LIBRARIES})

