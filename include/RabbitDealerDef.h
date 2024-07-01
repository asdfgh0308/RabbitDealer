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
#include "amqp.h"
#include "amqp_tcp_socket.h"
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


#define ERR_JUDGE_RET(rv)                                                                 \
    if (rv) {                                                                             \
        dzlog_error("ERROR ,%s:%d,%s ,ret: %d %x", __FILE__, __LINE__, __func__, rv, rv); \
        return rv;                                                                        \
    }

#define RABBIT_ERR_JUDGE_RET(reply, conn)                                \
    reply = amqp_get_rpc_reply(conn);                                    \
    if (reply.reply_type != AMQP_RESPONSE_NORMAL) {                      \
        dzlog_error("RABBITMQ ERROR! reply_type: %d", reply.reply_type); \
        return reply.reply_type;                                         \
    }

// RabbitMQ的管理器
namespace RabbitDealer {

using std::string;
using std::string_view;
typedef std::function<int(const string &)> handleMsgCallBack;
const char SYNCMSG_SEPARATOR[] = "\r\n";
constexpr int MSGID_BYTE_LENGTH = 8;
constexpr int MSGID_HEX_LENGTH = 16;


// 是否使用共享锁
#define USING_SHARED_MUTEX
#ifdef USING_SHARED_MUTEX
typedef std::shared_mutex RealMutex;
typedef std::shared_lock<RealMutex> RealLock;
#else
typedef std::mutex RealMutex;
typedef std::lock_guard<RealMutex> RealLock;
#endif

// #define RD_DEBUG

// 绑定交换机、路由键、队列名结构
struct StQueueBind {
    string queueName;
    string exchange;
    string routeKey;
};

}  // namespace RabbitDealer