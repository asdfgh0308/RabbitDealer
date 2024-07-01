#pragma once

#include <assert.h>
#include <atomic>
#include <functional>
#include <future>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include "RDUtils.h"
#include "amqp.h"
#include "amqp_tcp_socket.h"
#include "zlog.h"
#include "RabbitDealerDef.h"

// RabbitMQ的管理器
namespace RabbitDealer {

//前向声明
class RabbitDealer;

// Rabbit连接类。一个连接仅使用一个通道，关键接口由互斥锁控制并发
class RabbitConn {
public:
    RabbitConn(int type, int autoAck, int prefetch, int heartbeat, std::shared_ptr<RabbitDealer> rd);
    ~RabbitConn();
    // 创建/重建连接、通道
    int initConn();
    // 关闭连接
    int closeConn(int type = 0);
    // 断线、错误重连
    int reConn();
    // 延迟关闭连接，由外部线程操作，主要用于打断消费循环
    int outerCloseConn();
    //获取连接运行状态
    int getStatus();


    // 消费连接的属性设置
    int setConsumerInfo(const string &strQueueName, int prefetch, handleMsgCallBack cb = nullptr);
    // 绑定队列到消费连接
    int bindConsumerQueueToConn();

    // 声明交换机
    int declareExchange(const string &strExchange, const string &strType);
    // 声明队列, type=0 消息不过期， type=1 5秒消息过期
    int declareQueue(const string &strQueueName, int type = 0);
    // 绑定交换机、路由键到队列
    int bindQueueInServer(const string &strQueueName, const string &strExchange, const string &strRouteKey);

    // 获取队列消息数量
    int getQueueSize(const string &strQueueName, int &size);
    //清空队列消息
    int purgeQueue(const string &strQueueName);

    // 接收消息，线程安全版本
    // timeout.tv_sec: >=0 则指定阻塞超时，读不到就返回 , <0 循环至读取到结果
    int consumeMsgSafe(std::string &strMessage, const timeval &timeout = {-1, 0});
    // 接收一条消息
    // timeout.tv_sec: >=0 则指定阻塞超时，读不到就返回 , <0 循环至读取到结果
    int consumeMsg(std::string &strMessage, const timeval &timeout = {-1, 0});

    //--------------发送相关-------------
    // 发送一条消息，Lock=true有锁，线程安全;isHeartbeat=true是心跳消息，不打印日志
    template <bool lock, bool isHeartbeat>
    int publishMsg(const string_view &strMsgView, const string &strExchange, const string &strRoutekey);
    // 发送一条心跳消息，心跳队列消息存在5秒
    int publishHeartbeatMsg();


private:
    amqp_connection_state_t m_conn;
    amqp_socket_t *m_sock;

    int m_channelNum;       // =1
    int m_type;             // 0-publisher 1-consumer-autoack 2-consumer-noautoack
    string m_strQueueName;  // 仅有consumer连接有这个属性

    std::atomic<int> m_status;  // 0-uninitialized, 1-running, 2-closed
    int m_autoAck;              // 0-autoack, 1-noautoack
    int m_prefetchCount;        // 接收手动ack时，预取消息数量
    int m_heartbeat;            // 0-disable , >0-heartbeat time
    std::mutex m_usingMutex;    // rabbitmq-c关键接口限制并发
    handleMsgCallBack m_cb;
    std::weak_ptr<RabbitDealer> m_rd;
#ifdef RD_DEBUG
    static std::atomic<int> connCount;
#endif
};

}  // namespace RabbitDealer