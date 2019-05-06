//
// Created by mauro on 5/5/19.
//

#ifndef RACCOON_KAFKAPRODUCER_H
#define RACCOON_KAFKAPRODUCER_H

#include <string>
#include <csignal>
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include "iostream"
#include <fstream>
#include "librdkafka/rdkafka.h"
#include "librdkafka/rdkafkacpp.h"

using namespace std;

#define FALSE         0
#define TRUE          1
class KafkaProducer{
public:
    KafkaProducer(const string& brokers, const string& topics/*, int32_t partition*/);
    KafkaProducer();
    virtual ~KafkaProducer();
    /*
     The callback function is called once per message, indicating that the message was successfully delivered (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR)
     Still failed to pass (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR)
     The callback function is triggered by rd_kafka_poll() and executed on the thread of the application.
    */
    //void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque);//Callback
    bool PutBrokers(string);
    bool PutTopics(string);

    bool initKafka();
    bool initKafka(void(*dr_msg_cb)(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque));
    bool produce(string& mes, int32_t partition);//Run the producer to produce the mes to the specified partition of the CI
    //bool produce();
private:
    rd_kafka_t *rk;            /*Producer instance handle*/
    rd_kafka_topic_t *rkt;     /*Topic object*/
    rd_kafka_conf_t *conf;
    char errstr[512];
    string brokers;
    string topics;
    //int32_t partition;
    int run = 1;
};

#endif //RACCOON_KAFKAPRODUCER_H
