#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include "RDUtils.h"
#include "RabbitDealer.h"
#include "amqp.h"
#include "amqp_tcp_socket.h"
#include "zlog.h"


namespace RabbitDealer {

#ifdef RD_DEBUG
std::atomic<int> RabbitConn::connCount(0);
#endif


//--------------功能函数-------------
RabbitConn::RabbitConn(int type, int autoAck, int prefetch, int heartbeat, std::shared_ptr<RabbitDealer> rd) {
    m_strQueueName = "";
    m_conn = nullptr;
    m_sock = nullptr;
    m_channelNum = 1;
    m_type = type;
    m_status.store(0, std::memory_order_release);
    m_autoAck = autoAck;
    m_prefetchCount = prefetch;
    m_heartbeat = heartbeat;
    m_cb = nullptr;
    m_rd = rd;
#ifdef RD_DEBUG
    if (m_type == 0) connCount += 1;
    printf("After RabbitConnCtr count: %d\n", connCount.load(std::memory_order_relaxed));
#endif
}
RabbitConn::~RabbitConn() {
    closeConn(1);
#ifdef RD_DEBUG
    if (m_type == 0) connCount -= 1;
    printf("After ~RabbitConn count: %d\n", connCount.load(std::memory_order_relaxed));
#endif
}
// 只创建/重建连接、通道
int RabbitConn::initConn() {
    std::shared_ptr<RabbitDealer> parent_rd = m_rd.lock();
    if (parent_rd == nullptr) {
        dzlog_error("RabbitConn::init() parent_rd is null!");
        return ERR_RABBITMQ_CHANNELOPEN;
    }
    auto [szIP,iPort,szUser,szPasswd]=parent_rd->getLoginInfo();

    m_conn = amqp_new_connection();
    if (NULL == m_conn) {
        dzlog_error("amqp new connection failed!");
        return ERR_RABBITMQ_CONNCONNECT;
    }
    m_sock = amqp_tcp_socket_new(m_conn);
    if (NULL == m_sock) {
        dzlog_error("amqp tcp new socket failed");
        return ERR_RABBITMQ_CONNCONNECT;
    }
    int status = amqp_socket_open(m_sock, szIP.c_str(), iPort);
    if (status < 0) {
        dzlog_error("amqp socket open failed");
        return ERR_RABBITMQ_CONNCONNECT;
    }
    int ret =
         RDUtils::getErrorMsg(amqp_login(m_conn, "/", 0, 131072, m_heartbeat, AMQP_SASL_METHOD_PLAIN, szUser.c_str(), szPasswd.c_str()),
                 "Logging in");
    if (ret) {
        dzlog_error("amqp_login ERROR ,user:%s pw:%s", szUser.c_str(), szPasswd.c_str());
        return ERR_RABBITMQ_CONNCONNECT;
    }
    dzlog_debug("amqp_login success,user:%s pw:%s hb:%d prefetch:%d", szUser.c_str(), szPasswd.c_str(), m_heartbeat,
                m_prefetchCount);

    // 打开通道，一个连接一个通道，只用channel1
    m_channelNum = 1;
    amqp_channel_open(m_conn, m_channelNum);
    ret = RDUtils::getErrorMsg(amqp_get_rpc_reply(m_conn), "open channel");
    if (ret) {
        dzlog_error("Rabbitmq amqp_channel_open ERROR:num:%d, ret:%d!", m_channelNum, ret);
        // amqp_channel_close(m_conn, m_channelNum, AMQP_REPLY_SUCCESS);
        return ERR_RABBITMQ_CHANNELOPEN;
    }
    m_status.store(1, std::memory_order_release);
    return 0;
}
// 关闭连接,type=0： 重连前关闭，type=1 ：关闭连接
int RabbitConn::closeConn(int type) {
    // return 0;
    if (type==1){
        return 0;
        m_status.store(2, std::memory_order_release);
        std::lock_guard<std::mutex> lock(m_usingMutex);
        amqp_channel_close(m_conn, m_channelNum, AMQP_REPLY_SUCCESS);
        amqp_connection_close(m_conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(m_conn);
    }
    if (m_conn) {
        // std::lock_guard<std::mutex> lock(m_usingMutex);
        amqp_channel_close(m_conn, m_channelNum, AMQP_REPLY_SUCCESS);
        amqp_connection_close(m_conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(m_conn);
    }
    // string queuename=m_type>0?m_strQueueName:"public conn";
    // dzlog_warn("close conn queue:%s",queuename.c_str());
    return 0;
}
// 接收连接的属性设置
int RabbitConn::setConsumerInfo(const string &strQueueName, int prefetch, handleMsgCallBack cb) {
    if (m_type != 1 && m_type != 2) {
        return ERR_RABBITMQ_PARAMETER;
    }
    m_strQueueName = strQueueName;
    // 指定prefetchCount
    if (m_type == 2) {
        m_prefetchCount = prefetch;
    }
    if (cb) m_cb = cb;
    return 0;
}

// 延迟关闭连接，由外部线程操作，主要用于打断消费循环
int RabbitConn::outerCloseConn() {
    m_status.store(2, std::memory_order_release);  // = 2;
    return 0;
}
//获取连接运行状态
int RabbitConn::getStatus() {
    return m_status.load(std::memory_order_acquire);
}

// 断线、错误重连
int RabbitConn::reConn() {
    int ret = closeConn();
    ERR_JUDGE_RET(ret);
    ret = initConn();
    ERR_JUDGE_RET(ret);
    // 如果是个接收连接（也可能要发送，比如心跳包），则要重新绑定消费队列
    if (m_type > 0) {
        ret = bindConsumerQueueToConn();
        ERR_JUDGE_RET(ret);
    }
    return 0;
}

// 绑定队列到消费连接
int RabbitConn::bindConsumerQueueToConn() {
    // 此连接是消费者连接才需要绑定
    if (m_type == 0) {
        dzlog_error("bindConsumerQueueToConn bind ERROR, it's publish conn");
        return ERR_RABBITMQ_PARAMETER;
    }
    std::shared_ptr<RabbitDealer> parent_rd = m_rd.lock();
    amqp_rpc_reply_t reply;
    if (m_autoAck == 1) {  // auto ack
        amqp_basic_consume(m_conn, m_channelNum, amqp_cstring_bytes(m_strQueueName.c_str()), amqp_empty_bytes, 0, 1, 0, amqp_empty_table);
        RABBIT_ERR_JUDGE_RET(reply, m_conn);
    } else if (m_autoAck == 0) {                                      // no auto ack
        amqp_basic_qos(m_conn, m_channelNum, 0, m_prefetchCount, 0);  // 预取消息最多m_prefetchCount条
        RABBIT_ERR_JUDGE_RET(reply, m_conn);
        amqp_basic_consume(m_conn, m_channelNum, amqp_cstring_bytes(m_strQueueName.c_str()), amqp_empty_bytes, 0, 0, 0, amqp_empty_table);
        RABBIT_ERR_JUDGE_RET(reply, m_conn);
    }
    return 0;
}
// 获取队列消息数量
int RabbitConn::getQueueSize(const string &strQueueName, int &size) {
    if (getStatus() != 1) {
        dzlog_error("getQueueSize m_pConn status ERROR");
        return ERR_RABBITMQ_QUEUEBIND;
    }
    amqp_bytes_t _queue = amqp_cstring_bytes(strQueueName.c_str());
    int _passive = 1;
    int _durable = 1;
    int _exclusive = 0;
    int _auto_delete = 0;
    amqp_queue_declare_ok_t *qd;
    {
        std::lock_guard<std::mutex> lock(m_usingMutex);
        qd = amqp_queue_declare(m_conn, m_channelNum, _queue, _passive, _durable, _exclusive, _auto_delete, amqp_empty_table);
        if (0 != RDUtils::getErrorMsg(amqp_get_rpc_reply(m_conn), "queue_declare")) {
            // amqp_channel_close(pConn->conn, pConn->channelNum, AMQP_REPLY_SUCCESS);
            return ERR_RABBITMQ_QUEUEBIND;
        }
    }
    size = qd->message_count;
    dzlog_debug("getQueueSize queue:%s, message_count: %d consumer_count: %d", strQueueName.c_str(), qd->message_count, qd->consumer_count);
    return 0;
}
//清空队列消息
int RabbitConn::purgeQueue(const string &strQueueName) {
    int rv = 0;

    if (getStatus() != 1) {
        dzlog_error("PurgeQueue m_pConn status ERROR");
        return ERR_RABBITMQ_QUEUEBIND;
    }

    amqp_rpc_reply_t reply;
    amqp_bytes_t _queue = amqp_cstring_bytes(strQueueName.c_str());
    int _passive = 1;
    int _durable = 1;
    int _exclusive = 0;
    int _auto_delete = 0;
    {
        std::lock_guard<std::mutex> lock(m_usingMutex);
        amqp_queue_declare_ok_t *qd =
            amqp_queue_declare(m_conn, m_channelNum, _queue, _passive, _durable, _exclusive, _auto_delete, amqp_empty_table);
        if (0 != RDUtils::getErrorMsg(amqp_get_rpc_reply(m_conn), "queue_declare")) {
            amqp_channel_close(m_conn, m_channelNum, AMQP_REPLY_SUCCESS);
            dzlog_error("amqp_queue_declare");
            return ERR_RABBITMQ_QUEUEBIND;
        }
        amqp_queue_purge_ok_t *purge_ok = amqp_queue_purge(m_conn, m_channelNum, _queue);
        reply = amqp_get_rpc_reply(m_conn);
        if (0 != RDUtils::getErrorMsg(amqp_get_rpc_reply(m_conn), "purge_queue")) {
            amqp_channel_close(m_conn, m_channelNum, AMQP_REPLY_SUCCESS);
            dzlog_error("amqp_queue_declare");
            return ERR_RABBITMQ_QUEUEBIND;
        }
    }
    dzlog_debug("purge queue %s success", strQueueName.c_str());

    return rv;
}

// 声明交换机
int RabbitConn::declareExchange(const string &strExchange, const string &strType) {
    int ret = 0;
    if (getStatus() != 1) {
        dzlog_error("declareExchange m_pConn status ERROR");
        return ERR_RABBITMQ_QUEUEBIND;
    }

    amqp_bytes_t _exchange = amqp_cstring_bytes(strExchange.c_str());
    amqp_bytes_t _type = amqp_cstring_bytes(strType.c_str());
    int _passive = 0;
    int _durable = 1;  // 交换机是否持久化
    std::lock_guard<std::mutex> lock(m_usingMutex);
    amqp_exchange_declare(m_conn, m_channelNum, _exchange, _type, _passive, _durable, 0, 0, amqp_empty_table);
    //..todo 添加重试机制
    ret = RDUtils::getErrorMsg(amqp_get_rpc_reply(m_conn), "exchange_declare");
    if (ret) {
        dzlog_error("exchange_declare error exchange: %s ,ret %d", strExchange.c_str(), ret);
        return ERR_RABBITMQ_QUEUEBIND;
    }
    dzlog_debug("exchange_declare success exchange: %s", strExchange.c_str());
    return 0;
}
// 声明队列, type=0 消息不过期， type=1 5秒消息过期
int RabbitConn::declareQueue(const string &strQueueName, int type) {
    if (getStatus() != 1) {
        dzlog_error("QueueDeclare m_pConn status ERROR");
        return ERR_RABBITMQ_QUEUEBIND;
    }
    amqp_bytes_t _queue = amqp_cstring_bytes(strQueueName.c_str());
    int _passive = 0;
    int _durable = 1;
    int _exclusive = 0;
    int _auto_delete = 0;
    amqp_table_t args;
    if (type == 0) {
        args = amqp_empty_table;
    } else {
        RDUtils::amqpTableInit(&args, 1);
        RDUtils::amqpTableSetInt(&args, 0, "x-message-ttl", 5000);  // 过期5秒
        // amqp_table_set_str ( &args, 0, "x-dead-letter-exchange", dexchange); // 绑定死信交换器
        // amqp_table_set_str ( &args, 1, "x-dead-letter-routing-key", droutekey);
    }
    {
        std::lock_guard<std::mutex> lock(m_usingMutex);
        amqp_queue_declare(m_conn, m_channelNum, _queue, _passive, _durable, _exclusive, _auto_delete, args);
        //..todo 添加重试机制
        if (0 != RDUtils::getErrorMsg(amqp_get_rpc_reply(m_conn), "queue_declare")) {
            if (type != 0) RDUtils::amqpTableFree(&args);
            return ERR_RABBITMQ_QUEUEBIND;
        }
    }
    if (type != 0) {
        RDUtils::amqpTableFree(&args);
    }
    dzlog_debug("exchange_queue success queue: %s", strQueueName.c_str());
    return 0;
}
// 绑定交换机、路由键到队列
int RabbitConn::bindQueueInServer(const string &strQueueName, const string &strExchange, const string &strRouteKey) {
    if (getStatus() != 1) {
        dzlog_error("rabbitmq bindQueueInServer m_pConn status ERROR");
        return ERR_RABBITMQ_CONNCONNECT;
    }
    amqp_bytes_t _queue = amqp_cstring_bytes(strQueueName.c_str());
    amqp_bytes_t _exchange = amqp_cstring_bytes(strExchange.c_str());
    amqp_bytes_t _routkey = amqp_cstring_bytes(strRouteKey.c_str());
    std::lock_guard<std::mutex> lock(m_usingMutex);
    amqp_queue_bind(m_conn, m_channelNum, _queue, _exchange, _routkey, amqp_empty_table);
    //..todo 添加重试机制
    if (0 != RDUtils::getErrorMsg(amqp_get_rpc_reply(m_conn), "queue_bind")) {
        dzlog_error("rabbitmq bind queue ERROR");
        return ERR_RABBITMQ_QUEUEBIND;
    }
    dzlog_debug("bindQueue success queue: %s", strQueueName.c_str());
    return 0;
}

//--------------接收相关-------------
// 接收消息，线程安全版本
int RabbitConn::consumeMsgSafe(std::string &strMessage, const timeval &timeout) {
    std::lock_guard<std::mutex> lock(m_usingMutex);
    int ret = consumeMsg(strMessage, timeout);
    return ret;
}
// 接收一条消息
// timeout.tv_sec: >=0 则指定阻塞超时，读不到就返回 , <0 循环至读取到结果
int RabbitConn::consumeMsg(std::string &strMessage, const timeval &timeout) {
    amqp_rpc_reply_t reply;
    amqp_envelope_t envelope;
    strMessage = "";
    // type1：timeout.tv_sec<0, 默认阻塞超时，读不到将继续循环
    struct timeval realTimeout {
        3, 0
    };
    // type2：指定超时,时限内读不到就返回
    if (timeout.tv_sec >= 0) {
        realTimeout = timeout;
    }
    int ret = 0;

    while (1) {
        amqp_maybe_release_buffers(m_conn);
        reply = amqp_consume_message(m_conn, &envelope, &realTimeout, 0);
        // 非超时异常，重连，重试一次
        if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION && reply.library_error != AMQP_STATUS_TIMEOUT) {
            dzlog_error("amqp_consume_message ERROR once ,reconnect, queuename: %s", m_strQueueName.c_str());
            ret = reConn();
            ERR_JUDGE_RET(ret);
            reply = amqp_consume_message(m_conn, &envelope, &realTimeout, 0);
        }
        if (reply.reply_type == AMQP_RESPONSE_NORMAL) {  // 成功
            dzlog_debug("amqp_consume_message received msglen:%lu", envelope.message.body.len);
            strMessage = std::move(string((char *)envelope.message.body.bytes, (char *)envelope.message.body.bytes + envelope.message.body.len));
            amqp_destroy_envelope(&envelope);
            // 收到消息 ，执行回调
            dzlog_debug("RabbitDealer Recv Queue:%s , msg: %s", m_strQueueName.c_str(), strMessage.c_str());
            if (m_cb) {
                m_cb(strMessage);
                dzlog_debug("Queue:%s Received msg callback over", m_strQueueName.c_str());
            }
            // 回调完再ack，防止消息丢失
            if (m_autoAck == 0) {
                amqp_basic_ack(m_conn, m_channelNum, envelope.delivery_tag, 1);  // noautoack,发送ack
                RABBIT_ERR_JUDGE_RET(reply, m_conn);
            }
            break;
        } else if (timeout.tv_sec < 0 && reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION &&
                   reply.library_error == AMQP_STATUS_TIMEOUT) {  // 默认阻塞超时，则继续循环
            // dzlog_debug("consumeMsg timeout, looping %s", m_strQueueName.c_str());
            if (getStatus() == 1)
                continue;
            else
                break;
        } else if (reply.library_error == AMQP_STATUS_TIMEOUT) {  // timeout.tv_sec >= 0 指定超时，无消息，则返回
            dzlog_warn("amqp_consume_message received timeout:%ld ,QueueName:%s", realTimeout.tv_sec, m_strQueueName.c_str());
            break;
        } else {  // 其它异常，第二次，失败
            dzlog_error("amqp_consume_message ERROR twice return,queuename:%s,err_type:%d,err_no:%d", m_strQueueName.c_str(), reply.reply_type,
                        reply.library_error);
            break;
        }
    }
    return ret;
}

//--------------发送相关-------------
// 发送一条消息，Lock=true有锁，线程安全;isHeartbeat=true是心跳消息，不打印日志
template <bool lock, bool isHeartbeat>
int RabbitConn::publishMsg(const string_view &strMsgView, const string &strExchange, const string &strRoutekey) {
    int ret = 0;
    amqp_rpc_reply_t reply;
    amqp_bytes_t message_bytes;
    message_bytes.len = strMsgView.length();
    message_bytes.bytes = (void *)(strMsgView.data());

    amqp_bytes_t exchange = amqp_cstring_bytes(strExchange.data());
    amqp_bytes_t routekey = amqp_cstring_bytes(strRoutekey.data());
    if constexpr (lock) {
        std::lock_guard<std::mutex> tlock(m_usingMutex);
        ret = amqp_basic_publish(m_conn, m_channelNum, exchange, routekey, 0, 0, nullptr, message_bytes);
        reply = amqp_get_rpc_reply(m_conn);
        if (ret || reply.reply_type != AMQP_RESPONSE_NORMAL) {
            dzlog_warn("amqp_basic_publish ERROR once! Exchange:%s ,RouteKey:%s, ret: %d , reply_type: %d", strExchange.data(), strRoutekey.data(),
                       ret, reply.reply_type);
            // 重连一次
            ret = reConn();
            ERR_JUDGE_RET(ret);

            ret = amqp_basic_publish(m_conn, m_channelNum, exchange, routekey, 0, 0, nullptr, message_bytes);
            reply = amqp_get_rpc_reply(m_conn);
            if (ret || reply.reply_type != AMQP_RESPONSE_NORMAL) {
                dzlog_error("amqp_basic_publish ERROR twice, return! Exchange:%s ,RouteKey:%s, ret: %d , reply_type: %d", strExchange.data(),
                            strRoutekey.data(), ret, reply.reply_type);
                return ERR_RABBITMQ_PUBLISHMSG;
            }
        }
    } else {
        ret = amqp_basic_publish(m_conn, m_channelNum, exchange, routekey, 0, 0, nullptr, message_bytes);
        reply = amqp_get_rpc_reply(m_conn);
        if (ret || reply.reply_type != AMQP_RESPONSE_NORMAL) {
            dzlog_warn("amqp_basic_publish ERROR once! Exchange:%s ,RouteKey:%s, ret: %d , reply_type: %d", strExchange.data(), strRoutekey.data(),
                       ret, reply.reply_type);
            // 重连一次
            ret = reConn();
            ERR_JUDGE_RET(ret);

            ret = amqp_basic_publish(m_conn, m_channelNum, exchange, routekey, 0, 0, nullptr, message_bytes);
            reply = amqp_get_rpc_reply(m_conn);
            if (ret || reply.reply_type != AMQP_RESPONSE_NORMAL) {
                dzlog_error("amqp_basic_publish ERROR twice, return! Exchange:%s ,RouteKey:%s, ret: %d , reply_type: %d", strExchange.data(),
                            strRoutekey.data(), ret, reply.reply_type);
                return ERR_RABBITMQ_PUBLISHMSG;
            }
        }
    }
    if constexpr (!isHeartbeat) {
        dzlog_debug("RabbitDealer publish msg success , Exchange:%s ,RouteKey:%s, msg:%s , ret: %d , reply_type: %d", strExchange.data(),
                    strRoutekey.data(), strMsgView.data(), ret, reply.reply_type);
    }
    return 0;
}

// 发送一条心跳消息，心跳队列消息存在5秒
int RabbitConn::publishHeartbeatMsg() {
    static const string_view strMsgView = "hb";
    static const string strExchange = "HeartbeatExchange";
    static const string strRoutekey = "HeartbeatQueue";

    int ret = publishMsg<true, true>(strMsgView, strExchange, strRoutekey);
    ERR_JUDGE_RET(ret);
    return 0;
}


template int RabbitConn::publishMsg<true,true>(const string_view &strMsgView, const string &strExchange, const string &strRoutekey);
template int RabbitConn::publishMsg<true,false>(const string_view &strMsgView, const string &strExchange, const string &strRoutekey);
template int RabbitConn::publishMsg<false,false>(const string_view &strMsgView, const string &strExchange, const string &strRoutekey);

}  // namespace RabbitDealer