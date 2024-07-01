#!/bin/bash

if [ $1 == "buildrmqc" ]; then
	mkdir -p lib
	cd third/rabbitmq-c-0.11.0
	mkdir -p build
	cd build && cmake .. && make  && cp librabbitmq/*.so* ../../../lib/ && echo "build rabbitmq-c success"
fi

if [ $1 == "build" ]; then
	echo "build "
	mkdir -p build
	cd ./build && cmake .. &&  make && echo "build RabbitDealer tester success"
fi

if [ $1 == "run" ]; then
	ps -ef | grep RabTester | grep -v grep | awk '{print $2}' |xargs kill -9
	export LD_LIBRARY_PATH=../lib:$LD_LIBRARY_PATH
	cd ./build && ./RabTester $2 $3
fi


