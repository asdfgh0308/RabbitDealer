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
#include "RabbitDealer.h"
#include "amqp.h"
#include "amqp_tcp_socket.h"
#include "zlog.h"


namespace RabbitDealer {

//public functions
//--------功能函数-----------
RabbitDealer::RabbitDealer(const string &szIp, int iPort, const string &szUser, const string &szPwd, int prefetchCount,
                           int heartbeat, int publishConnPoolNum)
    : m_szIP(szIp),
      m_iPort(iPort),
      m_szUser(szUser),
      m_szPasswd(szPwd),
      m_running(1),
      m_prefetchCount(prefetchCount),
      m_heartbeatEnable(heartbeat),
      m_logMqEnable(1),
      m_publishConnCreated(0),
      m_publishConnPoolNum(publishConnPoolNum),
      m_idGenerator(12, 15) {
    m_syncMsgMap.clear();
}

RabbitDealer::~RabbitDealer() {
    stopReceiverThreads();
#ifdef RD_DEBUG
    dzlog_error("RabbitDealer::~RabbitDealer() end");
#endif
}

// 创建并绑定交换机、路由键、队列。
// mode=1 direct模式，mode=2 topic模式 ,mode=3 fanout模式， mode=9 direct 5秒过期模式
int RabbitDealer::bindQueue(const std::string &strExchange, const std::string &strRouteKey,
                            const std::string &strQueueName, int mode) {
    int ret = 0;
    // local bind
    if (mode == 1) {
        StQueueBind queueBind;
        queueBind.exchange = strExchange;
        queueBind.routeKey = strRouteKey;
        queueBind.queueName = strQueueName;
        {
            std::lock_guard<RealMutex> lock(m_sendMapMutex);
            m_sendMap.insert({strQueueName, queueBind});
        }
    }

    // remote bind
    std::shared_ptr<RabbitConn> pConn = getPublishConn(0);
    if (!pConn) {
        dzlog_error("RabbitDealer publishMsg getPublishConn failed");
        return ERR_RABBITMQ_CONNCONNECT;
    }
    std::string modeStr;
    switch (mode) {
        case 1:
            modeStr = "direct";
            break;
        case 9:
            modeStr = "direct";
            break;
        case 2:
            modeStr = "topic";
            break;
        case 3:
            modeStr = "fanout";
            break;
        default:
            modeStr = "direct";
    }
    ret = pConn->declareExchange(strExchange, modeStr);
    ERR_JUDGE_RET(ret);
    if (mode == 1) {  // direct模式，绑定队列和交换机、路由键
        ret = pConn->declareQueue(strQueueName);
        ERR_JUDGE_RET(ret);
        ret = pConn->bindQueueInServer(strQueueName, strExchange, strRouteKey);
        ERR_JUDGE_RET(ret);
    } else if (mode == 2) {  // topic 模式不用绑定

    } else if (mode == 3) {  // fanout 模式不用绑定

    } else if (mode == 9) {  // 心跳队列，设置消息5秒过期
        ret = pConn->declareQueue(strQueueName, 1);
        ERR_JUDGE_RET(ret);
        ret = pConn->bindQueueInServer(strQueueName, strExchange, strRouteKey);
        ERR_JUDGE_RET(ret);
    }
    return 0;
}

// 获取队列消息数量,此接口开销大
int RabbitDealer::getQueueSize(const string &strQueueName, int &size) {
    std::shared_ptr<RabbitConn> pConn = getPublishConn(0);
    if (!pConn) {
        dzlog_error("RabbitDealer publishMsg getPublishConn failed");
        return ERR_RABBITMQ_CONNCONNECT;
    }
    return pConn->getQueueSize(strQueueName, size);
}

//删除队列消息,此接口开销大
int RabbitDealer::purgeQueue(const string &strQueueName) {
    std::shared_ptr<RabbitConn> pConn = getPublishConn(0);
    if (!pConn) {
        dzlog_error("RabbitDealer publishMsg getPublishConn failed");
        return ERR_RABBITMQ_CONNCONNECT;
    }
    return pConn->purgeQueue(strQueueName);
}

int RabbitDealer::setLogMqEnable(int enable) {
    m_logMqEnable = enable;
    return 0;
}

int RabbitDealer::getLogMqEnable() {
    return m_logMqEnable;
}

std::tuple<string, int, string, string> RabbitDealer::getLoginInfo() {
    return {m_szIP, m_iPort, m_szUser, m_szPasswd};
}

// 设置RabbitDealer运行状态
int RabbitDealer::setRunning(int nRunning) {
    m_running = nRunning;
    return m_running;
};

// 结束所有线程
int RabbitDealer::stopReceiverThreads() {
    if (m_running == 0) {
        return 0;
    }
    setRunning(0);
    m_publishConnCreated = 0;
    usleep(10000);
    
    for (auto it = m_recvConnVector.begin(); it != m_recvConnVector.end(); ++it) {
        (*it)->outerCloseConn();
    }
    m_recvConnVector.clear();
    
    for (auto it = m_recvThreadVector.begin(); it != m_recvThreadVector.end(); ++it) {
        it->join();
    }
    m_recvThreadVector.clear();

    if (m_heartbeatThread.joinable()) m_heartbeatThread.join();
    
    m_publishConnVec.clear();

    return 0;
}


// 创建心跳线程
int RabbitDealer::createHeartbeatThread() {
    if (m_heartbeatThread.joinable()) {
        dzlog_error("m_heartbeatThread create ERROR");
        return ERR_RABBITMQ_PARAMETER;
    }

    bindQueue("HeartbeatExchange", "HeartbeatQueue", "HeartbeatQueue", 9);
    std::thread threadHeartbeat([this]() { this->heartbeatLoop(); });

    m_heartbeatThread = std::move(threadHeartbeat);
    return 0;
}

//-------------发送相关------------------
// 创建发送连接、心跳线程
int RabbitDealer::createPublishConn() {
    int ret = 0;
    for (int i = 0; i < m_publishConnPoolNum; ++i) {
        std::shared_ptr<RabbitConn> pConn;
        ret = createRabbitConn(pConn, 0);
        ERR_JUDGE_RET(ret);
        m_publishConnVec.push_back(pConn);
    }
    m_publishConnCreated = 1;

    if (m_heartbeatEnable > 0) {
        ret = createHeartbeatThread();
        ERR_JUDGE_RET(ret);
    }
    return 0;
}
// 动态调整发送连接池大小
int RabbitDealer::resizePublishConn(int connNum) {
    if (connNum <= 0) {
        dzlog_warn("RabbitDealer resizePublishConn connNum <= 0");
        return ERR_RABBITMQ_PARAMETER;
    }

    std::lock_guard<std::mutex> lock(m_resizePublishConnMutex);
    if (connNum == m_publishConnPoolNum) {
        // do nothing;
    } else if (connNum > m_publishConnPoolNum) {
        std::vector<std::shared_ptr<RabbitConn>> newVec;
        for (int i = 0; i < m_publishConnPoolNum; ++i) {
            newVec.push_back(m_publishConnVec[i]);
        }

        for (int i = m_publishConnPoolNum; i < connNum; ++i) {
            std::shared_ptr<RabbitConn> pConn;
            int ret = createRabbitConn(pConn, 0);
            ERR_JUDGE_RET(ret);
            newVec.push_back(pConn);
        }
        m_publishConnVec.swap(newVec);
        usleep(1000);
        m_publishConnPoolNum = connNum;
    } else {
        int t = m_publishConnPoolNum;
        m_publishConnPoolNum = connNum;
        usleep(1000);
        for (int i = connNum; i < t; ++i) {
            m_publishConnVec.pop_back();
        }
    }
    return 0;
}

// 发送消息，指定Exchange，RouteKey发送
int RabbitDealer::publishMsg(const string_view &strMessage, const string &strExchange, const string &strRoutekey) {
    std::shared_ptr<RabbitConn> pConn = getPublishConn();
    if (!pConn) {
        dzlog_error("RabbitDealer publishMsg getPublishConn failed");
        return ERR_RABBITMQ_CONNCONNECT;
    }
    return pConn->publishMsg<true, false>(strMessage, strExchange, strRoutekey);
}

// 发送消息，队列名作为参数的，队列名与Exchange RouteKey关系需要bindQueue来插入
int RabbitDealer::publishMsg(const string_view &strMessage, const string &strQueueName) {
    StQueueBind queueBind;
    {
        RealLock lock(m_sendMapMutex);
        auto it = m_sendMap.find(strQueueName);
        if (it == m_sendMap.end()) {
            dzlog_error("RabbitDealer publishMsg queuename not found:%s", strQueueName.c_str());
            return ERR_RABBITMQ_PUBLISHMSG;
        }
        queueBind = it->second;
    }
    std::shared_ptr<RabbitConn> pConn = getPublishConn();
    if (!pConn) {
        dzlog_error("RabbitDealer publishMsg getPublishConn failed");
        return ERR_RABBITMQ_CONNCONNECT;
    }
    return pConn->publishMsg<true, false>(strMessage, queueBind.exchange, queueBind.routeKey);
}

// 发送消息，无锁版本，需要指定连接id，关闭心跳，指定Exchange，RouteKey发送
int RabbitDealer::publishMsgNolock(const string_view &strMessage, const string &strExchange, const string &strRoutekey,
                                   int id) {
    // 0号通道用于队列创建等功能处理，因此禁用该通道，必须指定合法id
    if (id <= 0 || id >= m_publishConnPoolNum) {
        dzlog_error("RabbitDealer publishMsg connid error:%d", id);
        return ERR_RABBITMQ_PARAMETER;
    }
    std::shared_ptr<RabbitConn> pConn = getPublishConn(id);
    if (!pConn) {
        dzlog_error("RabbitDealer publishMsg getPublishConn failed");
        return ERR_RABBITMQ_CONNCONNECT;
    }
    return pConn->publishMsg<false, false>(strMessage, strExchange, strRoutekey);
}

//-------------接收相关---------------
// 自动接收，创建一个接收指定队列的线程，线程中创建一个新连接绑定到队列
// type: 1-consumer autoack,2-consumer noautoack
int RabbitDealer::createReceiverThread(const string &strQueueName, int type, handleMsgCallBack cb,
                                       int inputPrefetch, int id) {
    if (type != 1 && type != 2) {
        return ERR_RABBITMQ_PARAMETER;
    }
    // 创建接收连接
    std::shared_ptr<RabbitConn> pConn;
    int ret = createRabbitConn(pConn, type);
    if (ret) {
        dzlog_error("RabbitMQ createConn failed, %d", ret);
        return ret;
    }
    int prefetch = (type == 2 && inputPrefetch != -1) ? inputPrefetch : m_prefetchCount;
    ret = pConn->setConsumerInfo(strQueueName, prefetch, cb);
    ERR_JUDGE_RET(ret);
    ret = pConn->bindConsumerQueueToConn();
    ERR_JUDGE_RET(ret);

    // 启动循环接收线程
    std::thread threadMqRecvOne([this, pConn, id]() { this->consumeLoop(pConn, id); });
    // 保存连接、线程
    {
        std::lock_guard<std::mutex> lock(m_recvConnVectorMutex);
        m_recvConnVector.push_back(pConn);
    }
    {
        std::lock_guard<std::mutex> lock(m_recvThreadVectorMutex);
        m_recvThreadVector.push_back(std::move(threadMqRecvOne));
    }
    return 0;
}

// 主动接收，创建一个接收指定队列的连接，绑定到队列，不要与已有自动队列线程重复，否则消费顺序无法控制
// type: 1-consumer autoack,2-consumer noautoack
// inputPrefetch: 指定预取数量, -1采用默认 m_prefetchCount
int RabbitDealer::createReceiverQueue(const string &strQueueName, int type, int inputPrefetch, int id) {
    if (type != 1 && type != 2) {
        return ERR_RABBITMQ_PARAMETER;
    }

    int ret = 0;
    string msg;
    // 创建接收连接
    std::shared_ptr<RabbitConn> pConn;
    ret = createRabbitConn(pConn, type);
    if (ret) {
        dzlog_error("RabbitMQ createConn failed, %d", ret);
        return ret;
    }
    int prefetch = (type == 2 && inputPrefetch != -1) ? inputPrefetch : m_prefetchCount;
    ret = pConn->setConsumerInfo(strQueueName, prefetch);
    ERR_JUDGE_RET(ret);
    ret = pConn->bindConsumerQueueToConn();
    ERR_JUDGE_RET(ret);

    // 保存连接至成员map，后续需取用
    std::lock_guard<RealMutex> lock(m_recvPositiveConnMapMutex);
    m_recvPositiveConnMap[strQueueName] = std::move(pConn);
    return 0;
}

// 主动接收，取QueueName的连接，并接收一次消息，存放于Msg
int RabbitDealer::consumeMsgPositive(const string &strQueueName, string &msg, const timeval &tv, int id) {
    std::shared_ptr<RabbitConn> pConn;
    {
        // 查找queue对应的主动接受连接
        RealLock lock(m_recvPositiveConnMapMutex);
        if (m_recvPositiveConnMap.find(strQueueName) != m_recvPositiveConnMap.end()) {
            pConn = m_recvPositiveConnMap[strQueueName];
        } else {
            dzlog_error("RabbitDealer consumeMsgPositive queue not found:%s", strQueueName.c_str());
            return ERR_RABBITMQ_PARAMETER;
        }
    }
    int ret = pConn->consumeMsgSafe(msg, tv);
    if (ret) {
        dzlog_error("RabbitDealer consumeMsgPositive ERROR , queue:%s,ret %d", strQueueName.c_str(), ret);
        return ret;
    }
    dzlog_debug("RabbitDealer consumeMsgPositive success queue:%s,msg:%s", strQueueName.c_str(), msg.c_str());
    return 0;
}


//-------------同步消息---------------
// 发送同步消息，等待消息响应
// reqMsg 请求消息，函数将修改reqMsg，将msgid拼接在原消息尾部
// respMsg: 返回的响应消息
// msgId: 生成的消息id
int RabbitDealer::publishMsgSync(string reqMsg, string &respMsg, unsigned long long &msgId, const string &szExChange,
                                 const string &szRouteKey) {
    //生成msgid 拼接于原消息尾部，以"\r\n"分割
    msgId = m_idGenerator.genNextId();
    reqMsg += SYNCMSG_SEPARATOR;
    string idHexStr;
    idHexStr.resize(MSGID_HEX_LENGTH);
    int length = RDUtils::transByteToHex(reinterpret_cast<const unsigned char *>(&msgId), MSGID_BYTE_LENGTH,
                                         &idHexStr[0], MSGID_HEX_LENGTH);
    assert(length == MSGID_HEX_LENGTH);
    reqMsg += idHexStr;

    //利用future 同步获取响应消息
    std::promise<const string> promise;
    std::future<const string> future;
    future = promise.get_future();
    {
        //置入全局map
        //...todo 舍弃过期promise，需要记录时间戳
        std::lock_guard<std::mutex> lock(m_syncMsgMapMutex);
        m_syncMsgMap[msgId] = std::move(promise);
    }

    //发送消息
    int ret = publishMsg(reqMsg, szExChange, szRouteKey);
    ERR_JUDGE_RET(ret);
    dzlog_debug("publishMsgSync req msg over:%s ,id:%llu", reqMsg.c_str(), msgId);

    //等待future返回响应消息,3秒超时
    try {
        int count = 0;
        while (count < 1) {
            if (future.wait_for(std::chrono::seconds(3)) == std::future_status::ready) {
                break;
            }
            count++;
        }
        if (count >= 1) {
            dzlog_error("future timeout,id:%llu", msgId);
            return ERR_RABBITMQ_SYNCRESP;
        }
        respMsg = future.get();
        dzlog_debug("publishMsgSync get resp msg:%s ,id:%llu", respMsg.c_str(), msgId);
    } catch (std::exception &e) {
        dzlog_error("publishMsgSync get resp msg error:%s,id:%llu", e.what(), msgId);
    }
    return 0;
}

// 消费同步消息
// respMsg: 响应消息，函数会截取消息的主体部分
// msgId: 生成的消息id
// type=0: 通过promise同步发送端；type=1:仅拆分msg及msgId
int RabbitDealer::consumeMsgSync(string &respMsg, unsigned long long &msgId, int type) {
    // 拆分获取msgid
    int size = respMsg.size();
    if (respMsg[size - MSGID_HEX_LENGTH - 2] == '\r' && respMsg[size - MSGID_HEX_LENGTH - 1] == '\n') {  //是同步消息
        dzlog_debug("consumeMsgSync get Response msg :%s", respMsg.c_str());
    } else {  //普通消息
        dzlog_debug("get not Response msg:%s", respMsg.c_str());
        return 0;
    }
    string idHexStr = respMsg.substr(size - MSGID_HEX_LENGTH, MSGID_HEX_LENGTH);
    respMsg.resize(size - MSGID_HEX_LENGTH - 2);
    int length = RDUtils::transHexToByte(&idHexStr[0], MSGID_HEX_LENGTH, reinterpret_cast<unsigned char *>(&msgId),
                                         MSGID_BYTE_LENGTH);
    assert(length == MSGID_BYTE_LENGTH);
    dzlog_debug("consumeMsgSync get resp msg:%s ,id:%llu", respMsg.c_str(), msgId);
    if (type == 1) return 0;

    // type=0: 通过promise同步发送端
    try {
        std::promise<const string> promise;
        {
            std::lock_guard<std::mutex> lock(m_syncMsgMapMutex);
            auto it = m_syncMsgMap.find(msgId);
            if (it != m_syncMsgMap.end()) {
                promise = std::move(it->second);
                m_syncMsgMap.erase(it);
            } else {
                dzlog_error("consumeMsgSync can't find promise id:%llu", msgId);
                return ERR_RABBITMQ_SYNCRESP;
            }
        }
        promise.set_value(respMsg);
        dzlog_debug("consumeMsgSync set resp msg over, id:%llu", msgId);
    } catch (std::exception &e) {
        dzlog_error("consumeMsgSync set resp msg error:%s,id:%llu", e.what(), msgId);
    }
    return 0;
}

//private functions
//--------功能函数----------
// 创建RabbitConn对象，创建连接、通道，返回连接句柄
//  0-publisher, 1-consumer with autoack, 2-consumer with noautoack
int RabbitDealer::createRabbitConn(std::shared_ptr<RabbitConn> &pConn, int type) {
    int autoAck = type == 1 ? 1 : 0;
    pConn = std::make_shared<RabbitConn>(type, autoAck, m_prefetchCount, m_heartbeatEnable,
                                         shared_from_this());  // 创建新的连接对象
    int ret = pConn->initConn();
    ERR_JUDGE_RET(ret);

    dzlog_debug("Rabbitmq createConn success type:%d!", type);

    return 0;
}
// 心跳循环
int RabbitDealer::heartbeatLoop() {
    int ret = 0;
    int timeInterval = m_heartbeatEnable / 2 - 1;
    if (timeInterval < 1) timeInterval = 1;

    while (m_running) {
        std::this_thread::sleep_for(std::chrono::seconds(timeInterval));
        // 发送连接
        for (int i = 0; i < m_publishConnPoolNum; ++i) {
            std::shared_ptr<RabbitConn> pConn = getPublishConn(i);
            if (!pConn || pConn->getStatus() == 2) {
                continue;
            } else {
                ret = pConn->publishHeartbeatMsg();
                if (ret < 0) {
                    dzlog_error("amqp_heartbeat error:%d", ret);
                }
            }
        }
        // 主动接收连接
        RealLock lock(m_recvPositiveConnMapMutex);
        for (auto it = m_recvPositiveConnMap.begin(); it != m_recvPositiveConnMap.end(); it++) {
            std::shared_ptr<RabbitConn> pConn = it->second;
            if (pConn->getStatus() == 2) {
                continue;
            } else {
                // std::lock_guard<std::mutex> lockp(pConn->m_usingMutex);
                ret = pConn->publishHeartbeatMsg();
                if (ret < 0) {
                    dzlog_error("amqp_heartbeat error:%d", ret);
                }
            }
        }
    }
    return 0;
}


//-----------发送相关 -------------
// 获取一个连接，不指定id则随机获取。对于线程与连接绑定的，可以使用固定id
std::shared_ptr<RabbitConn> RabbitDealer::getPublishConn(int id) {
    if (m_publishConnCreated == 0) {
        dzlog_error("publish m_pConn is null, publish failed");
        return nullptr;
    }
    if (m_publishConnPoolNum == 1) return m_publishConnVec[0];
    if (id < 0 || id >= m_publishConnPoolNum) id = rand() % m_publishConnPoolNum;
    return m_publishConnVec[id];
};
//------------接收相关-------------

// 接收消息循环，线程中执行
int RabbitDealer::consumeLoop(std::shared_ptr<RabbitConn> pConn, int id) {
    int ret = 0;
    string msg;
    while (m_running) {
        // 此连接单线程使用，不需要加锁
        ret = pConn->consumeMsg(msg);
        if (m_running && ret) {
            std::this_thread::sleep_for(std::chrono::seconds(3));  // 有错误，等3秒再读
        }
    }
    return 0;
}
}  // namespace RabbitDealer