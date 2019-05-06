//
// Created by mauro on 5/6/19.
//

#ifndef RACCOON_KAFKACONSUMER_H
#define RACCOON_KAFKACONSUMER_H

#include <string>
#include <iostream>
#include "librdkafka/rdkafkacpp.h"
#include "librdkafka/rdkafka.h"
#include "KafkaProducer.h"
using namespace std;

#define MAX_BUF      65536
class KafkaConsumer
{
public:
    KafkaConsumer(const std::string& brokers, const std::string& topics, std::string groupid/*, int64_t offset*/);
    //kafka_consumer_client();
    virtual ~KafkaConsumer();
    //init all configuration of kafka
    bool initKafka();
    //
    string consume();
    bool consume_clear();

private:
    //Process and print the messages that have been consumed
    void msg_consume(rd_kafka_message_t *rkmessage, void *opaque);

    string brokers;
    string topics;
    string groupid;

    rd_kafka_t *rk;// Destructed
    rd_kafka_topic_t *rkt;
    rd_kafka_topic_partition_list_t *topics_list;//
    rd_kafka_conf_t *conf;
    rd_kafka_topic_conf_t *topic_conf;
    rd_kafka_resp_err_t err;
    /*
    int64_t last_offset_;
    RdKafka::Consumer *kafka_consumer_;
    RdKafka::Topic    *topic_;
    int64_t           offset_;
    //int32_t           partition;
    */
    int run;
    char tmp[16];
    char errstr[512];

};

#endif //RACCOON_KAFKACONSUMER_H
