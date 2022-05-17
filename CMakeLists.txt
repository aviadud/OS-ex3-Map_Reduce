cmake_minimum_required(VERSION 3.12)
project(ex3OS)

set(CMAKE_CXX_STANDARD 11)
set(ADDITIONAL_FLAGS "-Wall -pthread -g -I.")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ADDITIONAL_FLAGS}" )

add_executable(ex3OS Barrier.h Barrier.cpp MapReduceClient.h MapReduceFramework.h
        MapReduceFramework.cpp SampleClient.cpp )