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

std::shared_ptr<RabbitDealer::RabbitDealer> g_rdPtr;
extern int g_running;
extern string g_strIP;
extern int g_iPort;
extern string g_strUser;
extern string g_strPasswd;


RabbitDealer::TimerDealer tdPublisher[20100];
RabbitDealer::TimerDealer tdConsumer[20100];
int countArray[210][5];
double qpsArray[210][5];
double meanTime[210][5];
double maxTimeCost[210][5];

int hexPrint(char *p, int len) {
    for (int i = 0; i < len; ++i) {
        printf("%02x ", p[i]);
    }
    printf("\n");
    return 0;
}

//
int benchTaskSync(int tid, int count) {
    string szExChange = "NewTestExchange-" + std::to_string(tid);
    string szRouteKey = "NewTestQueue-" + std::to_string(tid);
    string szQueueName = "NewTestQueue-" + std::to_string(tid);
    RabbitDealer::RabbitDealer *rd = g_rdPtr.get();
    int ret = rd->bindQueue(szExChange, szRouteKey, szQueueName);
    rd->purgeQueue(szQueueName);
    ERR_JUDGE_RET(ret);

    tdConsumer[tid].start();
    ret = rd->createReceiverThread(
        szQueueName, 1,
        [rd, tid](const string &msg) {
            unsigned long long id;
            string newMsg = msg;
            rd->consumeMsgSync(newMsg, id);
            tdConsumer[tid].record();
            return 0;
        },
        15);
    ERR_JUDGE_RET(ret);

    for (int i = 0; i < count && g_running; ++i) {
        tdPublisher[tid].start();
        unsigned long long id;
        string reqMsg = std::to_string(tid) + ": Test Msg new ,id:" + std::to_string(i);
        string respMsg;
        ret = rd->publishMsgSync(reqMsg, respMsg, id, szExChange, szRouteKey);
        ERR_JUDGE_RET(ret);
        tdPublisher[tid].end();
    }
    return 0;
}

int benchTaskWithExAndRk(int tid, int count) {
    string szExChange = "NewTestExchange-" + std::to_string(tid);
    string szRouteKey = "NewTestQueue-" + std::to_string(tid);
    string szQueueName = "NewTestQueue-" + std::to_string(tid);
    RabbitDealer::RabbitDealer *rd = g_rdPtr.get();
    int ret = rd->bindQueue(szExChange, szRouteKey, szQueueName);
    rd->purgeQueue(szQueueName);
    ERR_JUDGE_RET(ret);

    tdConsumer[tid].start();
    ret = rd->createReceiverThread(
        szQueueName, 1,
        [tid](const string &msg) {
            tdConsumer[tid].record();
            return 0;
        },
        15);
    ERR_JUDGE_RET(ret);

    string msg = std::to_string(tid) + ": Test Msg new";
    for (int i = 0; i < count && g_running; ++i) {
        tdPublisher[tid].start();
        ret = rd->publishMsg(msg, szExChange, szRouteKey);
        ERR_JUDGE_RET(ret);
        tdPublisher[tid].end();
    }
    return 0;
}

int benchTaskWithQueueName(int tid, int count) {
    string szExChange = "NewTestExchange-" + std::to_string(tid);
    string szRouteKey = "NewTestQueue-" + std::to_string(tid);
    string szQueueName = "NewTestQueue-" + std::to_string(tid);
    RabbitDealer::RabbitDealer *rd = g_rdPtr.get();
    int ret = rd->bindQueue(szExChange, szRouteKey, szQueueName);
    rd->purgeQueue(szQueueName);
    ERR_JUDGE_RET(ret);

    tdConsumer[tid].start();
    ret = rd->createReceiverThread(
        szQueueName, 1,
        [tid](const string &msg) {
            tdConsumer[tid].record();
            return 0;
        },
        15);
    ERR_JUDGE_RET(ret);

    string msg = std::to_string(tid) + ": Test Msg new";
    for (int i = 0; i < count && g_running; ++i) {
        tdPublisher[tid].start();
        ret = rd->publishMsg(msg, szQueueName);
        ERR_JUDGE_RET(ret);
        tdPublisher[tid].end();
    }
    return 0;
}

int benchTaskNolock(int tid, int count) {
    string szExChange = "NewTestExchange-" + std::to_string(tid);
    string szRouteKey = "NewTestQueue-" + std::to_string(tid);
    string szQueueName = "NewTestQueue-" + std::to_string(tid);
    RabbitDealer::RabbitDealer *rd = g_rdPtr.get();
    int ret = rd->bindQueue(szExChange, szRouteKey, szQueueName);
    rd->purgeQueue(szQueueName);
    ERR_JUDGE_RET(ret);

    tdConsumer[tid].start();
    ret = rd->createReceiverThread(
        szQueueName, 1,
        [tid](const string &msg) {
            tdConsumer[tid].record();
            return 0;
        },
        15);
    ERR_JUDGE_RET(ret);

    string msg = std::to_string(tid) + ": Test Msg new";
    for (int i = 0; i < count && g_running; ++i) {
        tdPublisher[tid].start();
        ret = rd->publishMsgNolock(msg, szExChange, szRouteKey, tid + 1);
        ERR_JUDGE_RET(ret);
        tdPublisher[tid].end();
    }

    return 0;
}

int threadRabbitMqBenchmark(int tid, int count, std::function<int(int, int)> benchTask) {
    int ret;
    tdPublisher[tid].clear();
    tdConsumer[tid].clear();
    printf("tid:%d, count:%d run start\n", tid, count);
    benchTask(tid, count);
    printf("tid:%d, count:%d run end\n", tid, count);

    return 0;
}

int threadRabbitMqResize(int tid, int count) {
    RabbitDealer::RabbitDealer *rd = g_rdPtr.get();
    for (int i = 0; i < count && g_running; ++i) {
        sleep(5);
        if (!g_running) return 0;
        int t = rand() % 20 + 10;
        printf("resize publish conn to %d start\n", t);
        rd->resizePublishConn(t);
        printf("resize publish conn to %d end\n", t);
    }
    return 0;
}


int rabbitMqBenchmark(int threadNum, int count, std::function<int(int, int)> benchTask, int resize = 0) {
    printf("-----RabbitDealer: benchmark task start-------\n");

    memset(maxTimeCost, 0, sizeof(maxTimeCost));
    memset(meanTime, 0, sizeof(meanTime));
    memset(qpsArray, 0, sizeof(qpsArray));
    g_running = 1;

    int ret = 0;

    std::vector<std::thread> threads;
    g_rdPtr = std::make_shared<RabbitDealer::RabbitDealer>(g_strIP, g_iPort, g_strUser, g_strPasswd, 15, 0, threadNum+10);

    auto rd = g_rdPtr.get();
    ret = rd->createPublishConn();
    ERR_JUDGE_RET(ret);

    // 性能测试线程
    for (int i = 0; i < threadNum; i++) {
        threads.push_back(std::thread(threadRabbitMqBenchmark, i, count, benchTask));
    }
    // 扩容/缩容线程
    if (resize) {
        threads.push_back(std::thread(threadRabbitMqResize, 0, 2));
    }
    for (auto &t : threads) {
        t.join();
    }

    if (g_running == 0) {
        g_rdPtr.reset();
        return -1;
    }

    g_running = 0;
    g_rdPtr.reset();


    // 数据统计
    printf("calculating stastics...\n");
    for (int i = 0; i < threadNum; ++i) {
        // tdConsumer[tid].showInfo();
        countArray[i][0] = tdPublisher[i].getCount();
        qpsArray[i][0] = tdPublisher[i].getQps();
        meanTime[i][0] = tdPublisher[i].getMeanTimeCost();

        // tdConsumer[tid].showInfo();
        countArray[i][1] = tdConsumer[i].getCount();
        qpsArray[i][1] = tdConsumer[i].getQps();
        meanTime[i][1] = tdConsumer[i].getMeanTimeCost();
    }

    double totQps[10] = {};
    int totCount[10] = {};
    const char TypeString[2][20] = {"publisher", "consumer "};
    printf("thread stastics:\n");
    for (int i = 0; i < threadNum; i++) {
        printf("tid:%d\n", i);
        for (int j = 0; j < 2; ++j) {
            printf("    %s:count:%d, qps:%lf, meantime:%lf\n", TypeString[j], countArray[i][j], qpsArray[i][j], meanTime[i][j]);
            totQps[j] += qpsArray[i][j];
            totCount[j] += countArray[i][j];
        }
    }
    printf("total stastics:\n");
    for (int j = 0; j < 2; ++j) {
        printf("%s: totalcount:%d , totalqps:%lf\n", TypeString[j], totCount[j], totQps[j]);
    }
    printf("-----RabbitDealer: benchmark task end-------\n\n");

    return 0;
}

// 基准测试
int benchmark(int threadNum,int mcount) {
    int exid = 0;
    // int mcount = 500000;
    printf("---------RabbitDealer: benchmark() -----------------\n");

    printf("%d. -----RabbitDealer benchTaskSync -------\n", ++exid);
    rabbitMqBenchmark(threadNum, 10000, benchTaskSync);

    printf("%d. -----RabbitDealer benchTaskNolock -------\n", ++exid);
    rabbitMqBenchmark(threadNum, mcount, benchTaskNolock);

    printf("%d. -----RabbitDealer benchTaskWithExAndRk -------\n", ++exid);
    rabbitMqBenchmark(threadNum, mcount, benchTaskWithExAndRk);

    printf("%d. -----RabbitDealer benchTaskWithQueueName -------\n", ++exid);
    rabbitMqBenchmark(threadNum, mcount, benchTaskWithQueueName);

    printf("%d. -----RabbitDealer benchTaskResize -------\n", ++exid);
    rabbitMqBenchmark(threadNum, mcount, benchTaskWithQueueName, 1);

    return 0;
}
