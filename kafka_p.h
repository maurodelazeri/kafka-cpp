//
// Created by mauro on 9/13/19.
//
#pragma once

#include "unistd.h"
#include <iostream>
#include <sstream>

#include "librdkafka/rdkafka.h"

// Need to send a callback to use, generally do not need to send
class KafkaPCB
{
public:
    KafkaPCB() {};
    virtual ~KafkaPCB() {};

    // The user can print the log according to the example self-failure callback.
    virtual void p_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
        if (rkmessage->err) {
            std::cout << "kafka produce err, err_info="
                      << rd_kafka_err2str(rkmessage->err)
                      << "\n";
            //Own data，rkmessage->payload
            //Length of own data，rkmessage->len
        } else {
            std::cout << "kafka produce success, "
                      << "len="<< rkmessage->len
                      << ", partition=" << rkmessage->partition
                      << std::endl;
        }

    }
};

// Producer
class KafkaP
{
public:
    KafkaP();
    ~KafkaP();

    // des: initialization
    // Note: 1. The interface can only be called once, otherwise an error will occur and there is no init prevention.
    //       2.The message will be lost after 1s failure, and the queue will be lost within 1s (100000).
    //       3.If it is an important message, you must implement the callback yourself, and the result of sending the message will be returned in real time;
    // brokers,topic
    // err_info: Error message, correct for empty
    // clear_time_cout: Timeout cleaning time, will be lost after cleaning, but there will be callback notification after loss, default 1000ms
    // cb: Go back to the settings, there is no callback by default, if it is an important message, the send fails (the server is disconnected, etc.) or the callback is successful.
    bool init(const char* brokers, const char* topic, std::string& err_info, const char* clear_time_out = "1000", KafkaPCB* cb = NULL);

    // des: Safe shutdown
    void stop();

    // des: Send
    // Note: The length of the sent message only supports the length of 65535.
    // data: Message content
    // data_len: Message length
    // err_info: Error message, correct for empty, internal will clear
    // key: The default is null, the message is not guaranteed to be in order, the partition is random, set the key (such as userID), the key must be ordered
    // key_len: Set at the same time as the key
    // time_out：Send wait time, 0 means non-blocking
    bool produce(const char* data, uint16_t data_len, std::string& err_info, const char* key = NULL, uint16_t key_len = 0, uint32_t time_out = 0);

private:
    static void produce_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);
private:
    //base
    bool status_;
    std::string brokers_;
    std::string topic_;
    KafkaPCB* cb_;

    //rd
    rd_kafka_t* rdhandler_;
    rd_kafka_conf_t *rdconf_;

    //topic
    rd_kafka_topic_t *rdtopic_;
    rd_kafka_topic_conf_t *rdtopic_conf_;
};
