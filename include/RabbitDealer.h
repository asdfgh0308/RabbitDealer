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
#include "RabbitDealerDef.h"

#include "RabbitConn.h"
#include "zlog.h"

#define ERR_RABBITMQ_INITPUBLISHER                  0x00030001 // MQ初始化失败
#define ERR_RABBITMQ_PUBLISHMSG                     0x00030002 // MQ发送消息失败

#define ERR_RABBITMQ_CONNCONNECT                    0x00030003 // MQ connect连接失败
#define ERR_RABBITMQ_CHANNELOPEN                    0x00030004 // MQ channel打开失败
#define ERR_RABBITMQ_QUEUEBIND                      0x00030005 // 绑定队列失败
#define ERR_RABBITMQ_CONSUMEMSG                     0x00030006 // 消费消息失败
#define ERR_RABBITMQ_PARAMETER                      0x0003000F // 输入参数错误
#define ERR_RABBITMQ_REPLY                          0x0003000E // 回复错误
#define ERR_RABBITMQ_REG_FAILED                     0x00030007 // MQ注册失败
#define ERR_RABBITMQ_RECV_FAILED                    0x00030008 // MQ接收失败
#define ERR_RABBITMQ_SYNCRESP                       0x00031001 // MQ同步响应失败
#define ERR_RABBITMQ_SYNCREQ                        0x00031002 // MQ同步响应失败
#define ERR_RABBITMQ_UNKNOWN                        0x0003FFFF // 未知错误


// RabbitMQ的管理器
namespace RabbitDealer {

using std::string;
using std::string_view;
typedef std::function<int(const string &)> handleMsgCallBack;


// RabbitDealer主类，因内部类需要weak_ptr指向RD，必须采用shared_ptr管理
class RabbitDealer : public std::enable_shared_from_this<RabbitDealer> {
public:
    RabbitDealer() = delete;
    RabbitDealer(const RabbitDealer &rd) = delete;
    RabbitDealer &operator=(const RabbitDealer &rd) = delete;

    //--------功能函数-----------
    RabbitDealer(const string &szIp, int iPort, const string &szUser, const string &szPwd, int prefetchCount = 15,
                 int heartbeat = 20, int publishConnPoolNum = 50);
    ~RabbitDealer();

    // 创建并绑定交换机、路由键、队列。
    // mode=1 direct模式，mode=2 topic模式 ,mode=3 fanout模式， mode=9 direct 5秒过期模式
    int bindQueue(const std::string &strExchange, const std::string &strRouteKey, const std::string &strQueueName,
                  int mode = 1);


    // 获取队列消息数量,此接口开销大
    int getQueueSize(const string &strQueueName, int &size);

    //删除队列消息,此接口开销大
    int purgeQueue(const string &strQueueName);

    int setLogMqEnable(int enable);

    int getLogMqEnable();

    std::tuple<string, int, string, string> getLoginInfo();

    // 设置RabbitDealer运行状态
    int setRunning(int nRunning);

    // 结束所有线程
    int stopReceiverThreads();

    // 创建心跳线程
    int createHeartbeatThread();


    //-------------发送相关------------------
    // 创建发送连接、心跳线程
    int createPublishConn();

    // 动态调整发送连接池大小
    int resizePublishConn(int connNum);


    // 发送消息，指定Exchange，RouteKey发送
    int publishMsg(const string_view &strMessage, const string &strExchange, const string &strRoutekey);

    // 发送消息，队列名作为参数的，队列名与Exchange RouteKey关系需要bindQueue来插入
    int publishMsg(const string_view &strMessage, const string &strQueueName);

    // 发送消息，无锁版本，需要指定连接id，关闭心跳，指定Exchange，RouteKey发送
    int publishMsgNolock(const string_view &strMessage, const string &strExchange, const string &strRoutekey, int id);

    //-------------接收相关---------------
    // 自动接收，创建一个接收指定队列的线程，线程中创建一个新连接绑定到队列
    // type: 1-consumer autoack,2-consumer noautoack
    int createReceiverThread(const string &strQueueName, int type, handleMsgCallBack cb, int inputPrefetch = -1,
                             int id = 0);

    // 主动接收，创建一个接收指定队列的连接，绑定到队列，不要与已有自动队列线程重复，否则消费顺序无法控制
    // type: 1-consumer autoack,2-consumer noautoack
    // inputPrefetch: 指定预取数量, -1采用默认 m_prefetchCount
    int createReceiverQueue(const string &strQueueName, int type, int inputPrefetch = -1, int id = 0);
    // 主动接收，取QueueName的连接，并接收一次消息，存放于Msg
    int consumeMsgPositive(const string &strQueueName, string &msg, const timeval &tv = {-1, 0}, int id = 0);


    //-------------同步消息---------------
    // 发送同步消息，等待消息响应
    // reqMsg 请求消息，函数将修改reqMsg，将msgid拼接在原消息尾部
    // respMsg: 返回的响应消息
    // msgId: 生成的消息id
    int publishMsgSync(string reqMsg, string &respMsg, unsigned long long &msgId, const string &szExChange,
                       const string &szRouteKey);
    // 消费同步消息
    // respMsg: 响应消息，函数会截取消息的主体部分
    // msgId: 生成的消息id
    // type=0: 通过promise同步发送端；type=1:仅拆分msg及msgId
    int consumeMsgSync(string &respMsg, unsigned long long &msgId, int type = 0);


private:
    //--------功能函数----------
    // 创建RabbitConn对象，创建连接、通道，返回连接句柄
    //  0-publisher, 1-consumer with autoack, 2-consumer with noautoack
    int createRabbitConn(std::shared_ptr<RabbitConn> &pConn, int type);
    // 心跳循环
    int heartbeatLoop();

    //-----------发送相关 -------------
    // 获取一个连接，不指定id则随机获取。对于线程与连接绑定的，可以使用固定id
    std::shared_ptr<RabbitConn> getPublishConn(int id = -1);
    //------------接收相关-------------

    // 接收消息循环，线程中执行
    int consumeLoop(std::shared_ptr<RabbitConn> pConn, int id = 0);

private:
    // ---功能相关---
    string m_szIP;
    int m_iPort;
    string m_szUser;
    string m_szPasswd;
    int m_running;          //是否在运行中 0-否，1-是
    int m_prefetchCount;    //默认预取消息数
    int m_heartbeatEnable;  // 心跳间隔，>0则开启心跳
    int m_logMqEnable;

    // 心跳线程
    std::thread m_heartbeatThread;

    // ---接收相关---
    // 自动接收队列线程，一个线程使用一个连接，监听一个队列
    std::mutex m_recvThreadVectorMutex;
    std::vector<std::thread> m_recvThreadVector;
    std::mutex m_recvConnVectorMutex;
    std::vector<std::shared_ptr<RabbitConn>> m_recvConnVector;

    // 主动收取的队列
    // 主动接收连接锁
    RealMutex m_recvPositiveConnMapMutex;
    std::map<std::string, std::shared_ptr<RabbitConn>> m_recvPositiveConnMap;  //[queuename, conn]

    // ---发送相关---
    // 队列映射锁
    RealMutex m_sendMapMutex;
    std::map<string, StQueueBind> m_sendMap;  //[queuename, {queuename,exchange,routekey}]

    int m_publishConnCreated;  // 0:not created, 1:created
    // 发送连接数
    int m_publishConnPoolNum;
    std::vector<std::shared_ptr<RabbitConn>> m_publishConnVec;

    // 扩容/缩容锁
    std::mutex m_resizePublishConnMutex;

    //同步消息锁
    std::mutex m_syncMsgMapMutex;
    std::map<long long, std::promise<const string>> m_syncMsgMap;  //[queuename, conn]

    // 消息id生成器
    IDGenerator m_idGenerator;
};

}  // namespace RabbitDealer