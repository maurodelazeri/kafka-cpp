//
// Created by mauro on 9/13/19.
//

#pragma once

#include "cstring"
#include <string>
#include <sstream>
#include <iostream>

#include "librdkafka/rdkafka.h"

class KafkaC
{
public:
    KafkaC();
    ~KafkaC();

    // des:initialization
    // return: Is there an error?
    // brokers、topic、group
    // err_info: Return error if something goes wrong
    // consume_old: Only available if the group has not been consumed (commit record does not exist), false starts from the latest consumption, true starts from the earliest message
    // others: If a single consumer closes for a long time, the data that does not want to be consumed during startup needs to be replaced by a group.
    bool init(const char* brokers, const char* topic, std::string group, std::string& err_info, bool consume_old = false);

    // Consumption of kafka messages, there is a message to read all the time, if not, the internal time will block time_cout
    // return: Whether to get the message, it also returns false after timeout. It is necessary to judge whether the error is reported according to whether err_info is empty.
    // msg: Acquired message
    // err_info: Return error if something goes wrong
    // time_cout: Blocking time ms when there is no message ms
    bool consume(std::string& msg, std::string& err_info, uint32_t time_cout = 200);

private:
    bool status_;
    std::string brokers_;
    std::string topic_;
    std::string group_;

    rd_kafka_t* rd_kafka_;
    rd_kafka_conf_t *rd_kafka_conf_;

    rd_kafka_topic_t *rd_topic_;
    rd_kafka_topic_conf_t *rd_topic_conf_;

    rd_kafka_topic_partition_list_t* rd_topic_partition_list_;
    rd_kafka_topic_partition_t* rd_topic_partition_;
};