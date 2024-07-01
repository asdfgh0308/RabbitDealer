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
#include "test.h"
#include "zlog.h"
using std::string;

int tdNum = 1;
int g_running = 1;
extern std::shared_ptr<RabbitDealer::RabbitDealer> g_rdPtr;

//编译前，请修改下列RabbitMQ配置
string g_strIP = "127.0.0.1";
int g_iPort = 5672;
string g_strUser = "admin1";
string g_strPasswd = "123456";

static void signal_handler(int signal) {
    printf("signal_handler receive signal, deal start:%d\n", signal);
    if (g_running == 0) return;
    g_running = 0;
    g_rdPtr->stopReceiverThreads();
    printf("signal_handler receive signal, deal over:%d\n", signal);
    return;
}


int main(int argc, char **argv) {
#ifdef USING_ZLOG
    int ret = dzlog_init("../etc/zlog.conf", "Rab");
    if (ret) {
        printf("zlog init failed\n");
        return 0;
    }
#endif
    signal(SIGINT, signal_handler);  

    int threadNum;
    int mcount;
    if (argc == 1) {
        threadNum = 5;
    } else {
        threadNum = std::stoi(argv[1]);
    }
    if (argc <= 2) {
        mcount = 500000;
    } else {
        mcount = std::stoi(argv[2]);
    }

    example();

    benchmark(threadNum, mcount);


    return 0;
}
