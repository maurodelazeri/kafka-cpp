//
// Created by mauro on 5/5/19.
//
#include "KafkaProducer.h"

using namespace std;

static void dr_msg_cb(rd_kafka_t *rk,
                      const rd_kafka_message_t *rkmessage, void *opaque){
    if (rkmessage->err)
        fprintf(stderr, "%% Message delivery failed: %s\n",
                rd_kafka_err2str(rkmessage->err));
    /*else
        fprintf(stderr,
        "%% Message delivered (%zd bytes, "
        "partition %"PRId32")\n",
        rkmessage->len, rkmessage->partition);*/
}

KafkaProducer::KafkaProducer(const string& brokers, const string& topics/*, int32_t partition*/)
        :brokers(brokers),
         topics(topics)
{
    initKafka(*dr_msg_cb);
}
KafkaProducer::KafkaProducer()
{

}
KafkaProducer::~KafkaProducer()
{
    fprintf(stderr, "%% Flushing final message.. \n");
    rd_kafka_flush(rk, 10 * 1000);
    rd_kafka_topic_destroy(rkt);
    rd_kafka_destroy(rk);
}
bool KafkaProducer::PutBrokers(string broker)
{
    brokers = broker;
    return true;
}
bool KafkaProducer::PutTopics(string topic)
{
    topics = topic;
    return true;
}
bool KafkaProducer::initKafka()
{
    initKafka(*dr_msg_cb);
    return true;
}
bool KafkaProducer::initKafka(void(*dr_msg_cb)(rd_kafka_t *rk,const rd_kafka_message_t *rkmessage, void *opaque))
{
    string brokers = KafkaProducer::brokers;
    const char *topic = KafkaProducer::topics.c_str();

    conf = rd_kafka_conf_new();

    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk){
        fprintf(stderr, "%% Failed to create new producer:%s\n", errstr);
        return 1;
    }

    int found;
    while ((found = brokers.find(";")) != string::npos)
    {
        string broker = brokers.substr(0, found);
        brokers = brokers.substr(found+1);
        if (rd_kafka_brokers_add(rk, broker.c_str())==0)
            fprintf(stderr, "%% No valid brokers specified\n");
    }
    if (rd_kafka_brokers_add(rk, brokers.c_str()) == 0)
        fprintf(stderr, "%% No valid brokers specified\n");


    rkt = rd_kafka_topic_new(rk, topic, NULL);
    if (!rkt){
        fprintf(stderr, "%% Failed to create topic object: %s\n",
                rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_destroy(rk);
        return 1;
    }


    return 0;
}

bool KafkaProducer::produce(string& mes,int32_t partition) {
    while (run) {
        if (mes.empty()) {
            cout << "message is empty" << endl;
            break;
        }
        size_t len = mes.size();
        char *send_mes = &mes[0];

        retry:
        if (rd_kafka_produce(
                rkt,
                partition,
                RD_KAFKA_MSG_F_COPY,
                send_mes, len,
                NULL, 0,
                NULL) == -1) {
            if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                rd_kafka_poll(rk, 100);
                goto retry;
            }
        } else {
            std::cout << "Enqueued message (" << len << "bytes) for topic " << rd_kafka_topic_name(rkt) << std::endl;
        }
        rd_kafka_poll(rk, 0);
        break;
    }
    return 0;
}