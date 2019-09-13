//
// Created by mauro on 9/13/19.
//

#include "kafka_p.h"


KafkaP::KafkaP()
{
    status_ = false;
    cb_ = nullptr;
    rdhandler_ = nullptr;
    rdconf_ = nullptr;
    rdtopic_ = nullptr;
    rdtopic_conf_ = nullptr;
}

KafkaP::~KafkaP()
{}

void KafkaP::stop()
{
    status_ = false;

    sleep(2);
    rd_kafka_poll(rdhandler_, 0);

    rd_kafka_topic_destroy(rdtopic_);
    rd_kafka_destroy(rdhandler_);

}

bool KafkaP::init(const char* brokers, const char* topic, std::string& err_info, const char* clear_time_out, KafkaPCB* cb)
{
    err_info.clear();
    err_info.resize(1024);

    //base
    brokers_ = brokers;
    topic_ = topic;
    if (brokers_.empty()) {
        err_info = "Brokers empty";
        return false;
    }
    if (topic_.empty()) {
        err_info = "Topic empty";
        return false;
    }

    //conf create and set
    rdconf_ = rd_kafka_conf_new();

    if (rd_kafka_conf_set(rdconf_,
                          "bootstrap.servers",
                          brokers,
                          (char*)err_info.data(),
                          err_info.size()) != RD_KAFKA_CONF_OK){
        return false;
    }
    if (nullptr != cb) {
        cb_ = cb;
        // Set callback, no return type
        rd_kafka_conf_set_dr_msg_cb(rdconf_, KafkaP::produce_cb);
    }

    //handler create and set
    rdhandler_ = rd_kafka_new(RD_KAFKA_PRODUCER, rdconf_, (char*)err_info.data(), err_info.size());
    if (nullptr == rdhandler_) {
        return false;
    }
    if (rd_kafka_brokers_add(rdhandler_, brokers) == 0) {
        err_info = std::string("rd_kafka_brokers_add err, brokers=") + brokers;
        return false;
    }

    //topic conf
    rdtopic_conf_ = rd_kafka_topic_conf_new();
    /*
     * queue.buffering.max.messages，the largest message that the queue can hold, the default is 100000, it does not support setting itself here.
     * message.timeout.ms 300000，The sent message is sent immediately and the callback is successful. If it is not successful, it will not call back immediately. After the timeout, the callback will return a failure. The timeout time is set here. ms
     */
    if (rd_kafka_topic_conf_set(rdtopic_conf_,
                                "message.timeout.ms",
                                clear_time_out,
                                (char*)err_info.data(),
                                err_info.size()) != RD_KAFKA_CONF_OK) {
        return false;
    }

    //topic
    rdtopic_ = rd_kafka_topic_new(rdhandler_, topic, rdtopic_conf_);

    status_ = true;

    return true;
}

bool KafkaP::produce(const char* data, uint16_t data_len, std::string& err_info, const char* key, uint16_t key_len, uint32_t time_out)
{
    if (!status_) {
        rd_kafka_poll(rdhandler_, time_out);
        err_info = "KafkaP status is stopped, can not produce";
        return false;
    }
    err_info.clear();

    if (nullptr == data || data_len == 0) {
        err_info = "data is NULL";
        return false;
    }

    int i = 0;
    int ret = rd_kafka_produce(rdtopic_,
                               RD_KAFKA_PARTITION_UA,   //Select partition, here is random
                               RD_KAFKA_MSG_F_COPY,     //copy
                               (void*)data,             //Message content
                               (size_t)data_len,        //Message length
                               key,                     //key
                               key_len,                 //key_len
                               this                     //Own callback data pointer
    );

    if (0 != ret) {
        std::stringstream ss;
        ss << std::string("produce err, errno=") << errno
           << ", brokers=" << brokers_
           << ", topic=" << topic_;
        err_info = ss.str();
        rd_kafka_poll(rdhandler_, time_out);
        return false;
    }

    //It can be understood as blocking sending. During the timeout time, it will wait for the message to be sent. If time_out is 0, it will be sent completely asynchronously.
    rd_kafka_poll(rdhandler_, time_out);

    return true;
}

void KafkaP:: produce_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
    // Opaque is always null, suspected to be obsolete parameters
    // rkmessage->_private is the data pointer to send the message itself
    if (rkmessage->_private == nullptr) {
        std::cout << "rkmessage->_private = null, maybe producer have stopped\n";
        //pass，should not touch
    } else {
        auto* self = (KafkaP*)rkmessage->_private;
        if (self->cb_ != nullptr) {
            self->cb_->p_cb(rk, rkmessage, opaque);
        }
    }
}