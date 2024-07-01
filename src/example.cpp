#include <arpa/inet.h>
#include <assert.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <syslog.h>
#include <unistd.h>
#include <future>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>


#include "RDUtils.h"
#include "RabbitDealer.h"
#include "zlog.h"

using std::string;
extern int g_running;
extern string g_strIP;
extern int g_iPort;
extern string g_strUser;
extern string g_strPasswd;


// RabbitDealer使用示例
int example() {
    int exid = 0;
    printf("--------RabbitDealer: example()--------------------\n");

    //初始化，为演示无锁发送，关闭心跳
    std::shared_ptr<RabbitDealer::RabbitDealer> rdShPtr =
        std::make_shared<RabbitDealer::RabbitDealer>(g_strIP, g_iPort, g_strUser, g_strPasswd, 15, 0, 10);

    RabbitDealer::RabbitDealer *rd = rdShPtr.get();
    int ret = rd->createPublishConn();
    ERR_JUDGE_RET(ret);

    string szExChange = "testExchange";
    string szRouteKey = "testQueue";
    string szQueueName = "testQueue";

    //绑定队列，包含了声明交换机、队列，但不能与已存在的属性不同
    ret = rd->bindQueue(szExChange, szRouteKey, szQueueName);
    ERR_JUDGE_RET(ret);

    //创建自动接收消息线程
    printf("%d. -----createReceiverThread-------\n", ++exid);
    ret = rd->createReceiverThread(szQueueName, 2, [](const string &msg) {
        printf("ReceiverThread receive msg: %s\n", msg.c_str());
        return 0;
    });
    ERR_JUDGE_RET(ret);
    ret = rd->publishMsg("Hello RabbitDealer for receiver thread", szExChange, szRouteKey);
    ERR_JUDGE_RET(ret);
    usleep(10000);

    //创建主动接收连接
    printf("%d. -----createReceiverQueue-------\n", ++exid);
    string szExChangePositive = "testExchange";
    string szRouteKeyPositive = "testQueuePositive";
    string szQueueNamePositive = "testQueuePositive";
    ret = rd->bindQueue(szExChangePositive, szRouteKeyPositive, szQueueNamePositive);
    ERR_JUDGE_RET(ret);
    ret = rd->createReceiverQueue(szQueueNamePositive, 1);
    ERR_JUDGE_RET(ret);
    ret = rd->publishMsg("Hello RabbitDealer: consume msg positive", szExChangePositive, szRouteKeyPositive);
    ERR_JUDGE_RET(ret);
    //主动接收一条消息
    string positiveMsg;
    ret = rd->consumeMsgPositive(szQueueNamePositive, positiveMsg, {5, 0});
    ERR_JUDGE_RET(ret);
    printf("consume positiveMsg: %s\n", positiveMsg.c_str());


    printf("%d. -----publish-------\n", ++exid);
    //发送消息，线程安全
    ret = rd->publishMsg("Hello RabbitDealer with szExChange,szRouteKey", szExChange, szRouteKey);
    ERR_JUDGE_RET(ret);

    //发送消息，需要预先调用bindQueue绑定queue
    ret = rd->publishMsg("Hello RabbitDealer with szQueueName", szQueueName);
    ERR_JUDGE_RET(ret);

    //发送消息，无锁，线程不安全版本，需要保证仅有一个线程使用该id的连接，且必须关闭心跳
    ret = rd->publishMsgNolock("Hello RabbitDealer with id , no lock", szExChange, szRouteKey, 5);
    ERR_JUDGE_RET(ret);
    usleep(10000);

    //获取队列大小
    printf("%d. -----getQueueSize-------\n", ++exid);
    string szExChangeSize = "testExchange";
    string szRouteKeySize = "testQueueSize";
    string szQueueNameSize = "testQueueSize";
    ret = rd->bindQueue(szExChangeSize, szRouteKeySize, szQueueNameSize);
    ERR_JUDGE_RET(ret);
    ret = rd->publishMsg("Hello RabbitDealer for size", szExChangeSize, szRouteKeySize);
    ERR_JUDGE_RET(ret);
    ret = rd->publishMsg("Hello RabbitDealer for size", szExChangeSize, szRouteKeySize);
    ERR_JUDGE_RET(ret);
    usleep(10000);
    int queueSize = -1;
    rd->getQueueSize(szQueueNameSize, queueSize);
    ERR_JUDGE_RET(ret);
    printf("queue %s size:%d\n", szQueueNameSize.c_str(), queueSize);


    //删除队列消息
    printf("%d. -----purgeQueue-------\n", ++exid);
    rd->purgeQueue(szQueueNameSize);
    ERR_JUDGE_RET(ret);
    queueSize = -1;
    rd->getQueueSize(szQueueNameSize, queueSize);
    ERR_JUDGE_RET(ret);
    printf("After purgeQueue, queue %s size:%d\n", szQueueNameSize.c_str(), queueSize);


    //发送消息并同步等待回复（以msgId绑定）
    printf("%d. -----Sync Response-------\n", ++exid);
    //这里简化示例，消息直接发送至接收队列，实际应等待另一端的消息回复
    string szExChangeSync = "testExchange";
    string szRouteKeySync = "testQueueSync";
    string szQueueNameSync = "testQueueSync";
    rd->bindQueue(szExChangeSync, szRouteKeySync, szQueueNameSync);
    ERR_JUDGE_RET(ret);
    string syncMsg;
    unsigned long long msgId;
    //创建自动接收消息线程
    ret = rd->createReceiverThread(
        szQueueNameSync, 1,
        [&rd](const string &msg) {
            unsigned long long id;
            string newMsg = msg;
            //解析消息MsgID，同步结果至发送端
            rd->consumeMsgSync(newMsg, id);
            return 0;
        },
        15);
    //发送同步消息
    rd->publishMsgSync("Hello RabbitDealer for sync", syncMsg, msgId, szExChangeSync, szRouteKeySync);
    ERR_JUDGE_RET(ret);
    printf("publishMsgSync get resp msg: %s\n", syncMsg.c_str());

    printf("%d. -----RabbitDealer example over , wait for destructor , threads to stop-------\n\n", ++exid);
    return 0;
}
