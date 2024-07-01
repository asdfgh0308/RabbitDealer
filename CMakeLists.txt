cmake_minimum_required(VERSION 3.5.0)

project(RabbitDealer VERSION 1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(TARGET RabTester)

file(GLOB SOURCES ${CMAKE_SOURCE_DIR}/src/*.cpp)

include_directories(
	${CMAKE_SOURCE_DIR}/include
	${CMAKE_SOURCE_DIR}/third/rabbitmq-c-0.11.0/librabbitmq
)

add_compile_options(-g -O0 -Wall -Wextra -Wno-unused-variable -Wno-unused-parameter)

link_directories(${CMAKE_SOURCE_DIR}/lib)

find_library(LIB_RBMQ rabbitmq HINTS  ${CMAKE_SOURCE_DIR}/lib)
find_library(LIB_SSL ssl)
find_library(LIB_CRYPTO crypto)
find_library(LIB_PTHREAD pthread)

add_executable(${TARGET} ${SOURCES})


target_link_libraries(
	${TARGET}
	${LIB_SSL}
	${LIB_CRYPTO}
	${LIB_PTHREAD}
	${LIB_RBMQ}
	)

