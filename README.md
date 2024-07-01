# RabbitDealer : a CPP thread-safe manager of RabbitMQ basing on librabbitmq-c  


## Introduction

RabbitDealer是一个基于rabbitmq-c的简易、线程安全的C++ RabbitMQ管理器库。  
本库是对rabbitmq-c的C++封装，基于C++17语言标准。  
本库编写的主要目的是简化RabbitMQ的使用，根据业务实际，抽象出几种常用模式，提供简洁的调用接口，同时在原库基础上提供线程安全、异常重连、心跳等功能的封装。  
项目地址：
 - <https://github.com/asdfgh0308/RabbitDealer>


## Features
  
本库有以下特点：
- 配置简单：RabbitMQ可配置参数众多，本库接口封装关键配置属性，并在各接口间进行关联，以提供简单的使用模式，方便调用。
- 接口简洁：本库根据作者实际工作经验，封装了几种常见使用模式，详见example()函数。所有操作仅需要在RabbitDealer类对象中调用，用户无需存储管理连接、队列名、交换机绑定关系、线程等对象。
- 线程安全：本库主要函数接口保证线程安全，可满足连接池式的、多线程交叉使用；同时也提供无锁非线程安全连接调用接口，满足高性能需求的使用。
- 异常重连：本库处理了可能发生的各类异常，具备异常重连功能，当连接断开/异常时，自动尝试重连。
- 完善心跳：本库完善了原库不完全的心跳功能，高可用场景下，可避免虚连接导致的消息延迟/丢失。
- 并发控制：本库采用互斥锁、读写锁、原子变量等工具控制并发，封装的同时保证高性能。
- 动态扩容缩容: 提供无性能影响的发送连接池动态扩容/缩容能力。
- 单连接单通道：本库不使用多通道模式，可使用多连接提升吞吐量。
- 详细注释：关键功能实现、接口有注释说明。
- 仅依赖rabbitmq-c及该库相关依赖库，无其它依赖。

本库在rabbitMQ3.7，rabbitmq-c-0.11.0环境下编译测试。

## Geting Started

1. 安装rabbitmq-c库依赖库，包括openssl,crypto

2. 执行编译前，请根据RabbitMQ服务器配置，修改src/main.cpp中IP、Port、User、Password配置值。

3. 执行以下脚本，编译rabbitmq-c库

    sh deah.sh buildrmqc    

4. 执行以下脚本，将在build目录下进行编译

    sh deal.sh build

5. 执行测试，包括example和benchmark，  
参数表示5个发送线程，每个测试项单个发送线程将发送100000次消息。  
测试程序将统计发送/接收的qps、平均耗时等关键数据。

    sh deal.sh run 5 100000



## License

MIT License



