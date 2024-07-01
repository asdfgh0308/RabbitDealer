#pragma once

#include <arpa/inet.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <syslog.h>
#include <unistd.h>

#include <map>
#include <mutex>
#include <string>
#include <thread>

#include "RabbitDealerDef.h"
#include "amqp.h"
#include "zlog.h"


namespace RabbitDealer {

class RDUtils {
public:
    static int getErrorMsg(amqp_rpc_reply_t x, char const *context) {
        switch (x.reply_type) {
            case AMQP_RESPONSE_NORMAL:
                return 0;
            case AMQP_RESPONSE_NONE:
                dzlog_error("%s: missing RPC reply type!\n", context);
                break;
            case AMQP_RESPONSE_LIBRARY_EXCEPTION:
                dzlog_error("%s: %s %d", context, amqp_error_string2(x.library_error), x.library_error);
                break;
            case AMQP_RESPONSE_SERVER_EXCEPTION:
                switch (x.reply.id) {
                    case AMQP_CONNECTION_CLOSE_METHOD: {
                        amqp_connection_close_t *m = (amqp_connection_close_t *)x.reply.decoded;
                        dzlog_error("%s: server connection error %uh, message: %.*s", context, m->reply_code, (int)m->reply_text.len,
                                    (char *)m->reply_text.bytes);
                        break;
                    }
                    case AMQP_CHANNEL_CLOSE_METHOD: {
                        amqp_channel_close_t *m = (amqp_channel_close_t *)x.reply.decoded;
                        dzlog_error("%s: server channel error %uh, message: %.*s", context, m->reply_code, (int)m->reply_text.len,
                                    (char *)m->reply_text.bytes);
                        break;
                    }
                    default:
                        dzlog_error("%s: unknown server error, method id 0x%08X", context, x.reply.id);
                        break;
                }
                break;

            default:
                dzlog_error("%s: rabbitmq other error type:%d ", context, x.reply_type);
        }
        return ERR_RABBITMQ_REPLY;
    }

    static int amqpTableInit(amqp_table_t *pTable, int number) {
        if (number <= 0) return -1;
        amqp_table_entry_t *pEntry = (amqp_table_entry_t *)calloc(number, sizeof(amqp_table_entry_t));
        pTable->num_entries = number;
        pTable->entries = pEntry;
        return 0;
    }

    static int amqpTableFree(amqp_table_t *pTable) {
        pTable->num_entries = 0;
        if (pTable->entries != NULL) {
            free(pTable->entries);
            pTable->entries = NULL;
        }
        return 0;
    }

    static int amqpTableSetInt(amqp_table_t *pTable, int pos, const char *pKey, int nValue) {
        if (pos >= pTable->num_entries) return -1;

        amqp_table_entry_t entry;

        entry.key = amqp_cstring_bytes(pKey);
        entry.value.kind = AMQP_FIELD_KIND_U16;
        entry.value.value.i16 = nValue;

        memcpy(&pTable->entries[pos], &entry, sizeof(amqp_table_entry_t));

        return 0;
    }

#define NON_NUM '0'

    static int hex2num(char c) {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;  // 这里+10的原因是:比如16进制的a值为10
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        dzlog_error("unexpected char: %c", c);
        return NON_NUM;
    }

    // 0x61 0x62 0x63->"616263"s
    // 返回转换后Hex长度，错误返回小于0
    static int transByteToHex(const unsigned char *bytes, int inLen, char *hexStr, int outBufLen) {
        if (bytes == NULL || hexStr == NULL || (inLen << 1) > outBufLen) return -2;
        for (int i = 0; i < inLen; i++) {
            sprintf(hexStr, "%02x", bytes[i]);
            hexStr += 2;
            // dzlog_error("%d %d %s\n", i, len, str);
        }
        return inLen << 1;
    }

    //"616263"s  -> 0x61 0x62 0x63
    // 返回转换后Bytes长度，错误返回小于0
    static int transHexToByte(const char *hexStr, int inLen, unsigned char *bytes, int outBufLen) {
        if (hexStr == NULL || bytes == NULL || inLen > (outBufLen << 1) || (inLen & 1)) return -2;

        int result = 0;
        for (int i = 0; i < inLen; i += 2) {
            unsigned char highNibble = hex2num(hexStr[i]);
            unsigned char lowNibble = hex2num(hexStr[i + 1]);
            if (highNibble == NON_NUM || lowNibble == NON_NUM) {
                dzlog_error("invalid hex string: %d %d", hexStr[i], hexStr[i + 1]);
                return -1;
            }
            bytes[result++] = (highNibble << 4) | lowNibble;
        }
        return result;
    }
};

class TimerDealer {
public:
    TimerDealer() : count(0), totalTime(0) {}
    ~TimerDealer() {}
    int clear() {
        count = 0;
        totalTime = 0;
        return 0;
    }
    void start() { gettimeofday(&t1, NULL); }
    double end() {  // 每次开始，再结束，记录时间差
        gettimeofday(&t2, NULL);
        double timeCostOnce = ((t2.tv_sec * 1000000 + t2.tv_usec) - (t1.tv_sec * 1000000 + t1.tv_usec)) / 1000000.0;
        totalTime += timeCostOnce;
        count += 1;
        return timeCostOnce;
    }
    double record() {  // 只开始一次，每次记录当前距首次开始偏移时间
        gettimeofday(&t2, NULL);
        double timeCost = ((t2.tv_sec * 1000000 + t2.tv_usec) - (t1.tv_sec * 1000000 + t1.tv_usec)) / 1000000.0;
        totalTime = timeCost;
        count += 1;
        return timeCost;
    }

    double getQps() { 
        if (count == 0) return 0;
        if (totalTime<1e-9) return 0;
        return count / totalTime; 
    }
    double getMeanTimeCost() {
        if (count == 0) return -1;
        return totalTime / count;
    }
    int getCount() { return count; }
    double getTotalTime() { return totalTime; }
    int showInfo() {
        // std::thread::id threadID = std::this_thread::get_id();
        printf("Timer: count:%d, totalTime:%f, qps:%f, meanTimeCost:%f\n", count, totalTime, getQps(), getMeanTimeCost());
        return 0;
    }

private:
    int count;
    double totalTime;
    struct timeval t1, t2, t3;
};


class IDGenerator {
    typedef long long uint64_t;

public:
    IDGenerator(int datacenterId, int workerId) : m_dataCenterId(datacenterId), m_workerId(workerId), m_atomSeqID(0) {
        if (m_dataCenterId > maxDatacenterId_ || m_workerId > maxm_workerId) {
            dzlog_error("datacenterId or worker ID exceeds limit");
        }
    }

    uint64_t genNextId() {
        // std::unique_lock<std::mutex> lock(mutex_);
        uint64_t timestamp = currentTimestamp();
        if (timestamp + 100 < m_maxTimestamp) {
            // throw std::runtime_error("Clock moved backwards");
            dzlog_error("Clock moved backwards more than 100ms");
            return -1;
        }
        if (timestamp > m_maxTimestamp) {
            m_maxTimestamp = timestamp;
        }
        // ID的特殊设置：不随着时间更新而置零，这样在少量时间回拨时，不会导致ID重复
        uint64_t expected;
        expected = m_atomSeqID.fetch_add(1, std::memory_order_release);
        while (expected > sequenceMask_) {
            // 使用CAS操作来尝试更新变量
            uint64_t desired = expected & sequenceMask_;
            if (m_atomSeqID.compare_exchange_weak(expected, desired)) {
                // 如果CAS成功，我们已经完成了操作，可以退出循环
                break;
            }
            // 如果CAS失败，说明有其他线程修改了变量，我们需要重新读取并尝试
            expected = m_atomSeqID.load(std::memory_order_acquire);
        }

        return (((timestamp - unixEpoch_) & timestampMask_) << timestampLeftShift_) | ((m_dataCenterId & maxDatacenterId_) << datacenterIdShift_) |
               ((m_workerId & maxm_workerId) << workerIdShift_) | (expected & sequenceMask_);
    }

private:
    uint64_t currentTimestamp() const { return std::time(nullptr) * 1000; }
    // 总长度64位
    // timestampBits_：时间戳长度，
    // datacenterIdBits_：datacenterId长度，workerIdBits_:workerId长度
    // sequenceBits_:seqID长度
    const uint64_t sequenceBits_ = 24;
    const uint64_t sequenceMask_ = (1ULL << sequenceBits_) - 1;
    const uint64_t workerIdShift_ = sequenceBits_;

    const uint64_t workerIdBits_ = 5;
    const uint64_t datacenterIdBits_ = 5;
    const uint64_t maxm_workerId = (1ULL << workerIdBits_) - 1;
    const uint64_t maxDatacenterId_ = (1ULL << datacenterIdBits_) - 1;
    const uint64_t datacenterIdShift_ = sequenceBits_ + workerIdBits_;

    const uint64_t unixEpoch_ = 0ULL;  // Unix时间戳起点：1970-01-01 00:00:00 UTC 可修改
    const uint64_t timestampLeftShift_ = sequenceBits_ + workerIdBits_ + datacenterIdBits_;
    const uint64_t timestampBits_ = 64 - (sequenceBits_ + workerIdBits_ + datacenterIdBits_);
    const uint64_t timestampMask_ = (1ULL << timestampBits_) - 1;

    int m_dataCenterId;
    int m_workerId;
    std::atomic<uint64_t> m_atomSeqID;
    uint64_t m_maxTimestamp;
};

}  // namespace RabbitDealer
