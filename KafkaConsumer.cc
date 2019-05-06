//
// Created by mauro on 5/6/19.
//

#include "KafkaConsumer.h"

KafkaConsumer::KafkaConsumer(const std::string& brokers, const std::string& topics, std::string groupid/*, int64_t offset*/)
        :brokers(brokers),
         topics(topics),
         groupid(groupid)
//,offset_(offset)
{
    /*
    last_offset_ = 0;
    kafka_consumer_ = NULL;
    topic_ = NULL;
    offset_ = RD_KAFKA_OFFSET_BEGINNING;
    //int32_t           partition;
    */
    int run = 1;
    //initKafka();
}

KafkaConsumer::~KafkaConsumer()
{
/*This call will block until the consumer undoes its allocation, calling rebalance_cb (if it is set),
     Commit offset to the broker and leave the consumer group
     The maximum blocking time is set to session.timeout.ms
     */
    rd_kafka_resp_err_t err;

    err = rd_kafka_consumer_close(rk);
    if (err) {
        fprintf(stderr, "%% Failed to close consumer: %s\n", rd_kafka_err2str(err));
    }
    else {
        fprintf(stderr, "%% Consumer closed\n");
    }

    //release all the resources used by the topics list and it's own
    rd_kafka_topic_partition_list_destroy(topics_list);

    //destroy kafka handle
    rd_kafka_destroy(rk);

    run = 5;
    //wait for all rdkafka t objects to be destroyed, all kafka objects are destroyed, return 0, timeout returns -1
    while (run-- > 0 && rd_kafka_wait_destroyed(1000) == -1){
        printf("Waiting for librdkafka to decommission\n");
    }
    if (run <= 0){
        //Dump rdkafka internal state to stdout stream
        rd_kafka_dump(stdout, rk);
    }
}


void KafkaConsumer::msg_consume(rd_kafka_message_t *rkmessage,
                                void *opaque) {
    if (rkmessage->err) {

        if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
            rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
            run = 0;
        return;
    }

    if (rkmessage->key_len) {
        printf("Key: %.*s\n",
               (int)rkmessage->key_len, (char *)rkmessage->key);
    }
    /*
    printf("%.*s\n",
        (int)rkmessage->len, (char *)rkmessage->payload);
    */
    return;
}

/*
init all configuration of kafka
*/
bool KafkaConsumer::initKafka()
{
    string brokers = KafkaConsumer::brokers;
    const char *group = KafkaConsumer::groupid.c_str();
    const char *topic = KafkaConsumer::topics.c_str();



    // Kafka configuration
    conf = rd_kafka_conf_new();

    //quick termination
    //snprintf(tmp, sizeof(tmp), "%i", SIGIO);
    rd_kafka_conf_set(conf, "internal.termination.signal", tmp, NULL, 0);

    //topic configuration
    topic_conf = rd_kafka_topic_conf_new();

    // Consumer groups require a group id
    if (!group)
        group = "rdkafka_consumer_example";
    if (rd_kafka_conf_set(conf, "group.id", group,
                          errstr, sizeof(errstr)) !=
        RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%% %s\n", errstr);
        return FALSE;
    }

    //* Consumer groups always use broker based offset storage
    if (rd_kafka_topic_conf_set(topic_conf, "offset.store.method",
                                "broker",
                                errstr, sizeof(errstr)) !=
        RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%% %s\n", errstr);
        return FALSE;
    }
    /*
    if (rd_kafka_topic_conf_set(topic_conf, "auto.offest.reset",
        "smallest",//largest
        errstr, sizeof(errstr)) !=
        RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%% %s\n", errstr);
        return FALSE;
    }
*/
    //* Set default topic config for pattern-matched topics.
    rd_kafka_conf_set_default_topic_conf(conf, topic_conf);


    // Instantiate a top-level object rdkafka t as a base container, providing global configuration and sharing status
    rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk){
        fprintf(stderr, "%% Failed to create new consumer:%s\n", errstr);
        return FALSE;
    }
    /* Create topic */
    rkt = rd_kafka_topic_new(rk, topic, topic_conf);

    //Librdkafka needs at least one broker's initialization list
    int found;
    while ((found = brokers.find(";")) != string::npos)
    {
        string broker = brokers.substr(0, found);
        brokers = brokers.substr(found+1);
        if (rd_kafka_brokers_add(rk, broker.c_str()) == 0)
            fprintf(stderr, "%% No valid brokers specified\n");
    }
    if (rd_kafka_brokers_add(rk, brokers.c_str()) == 0)
        fprintf(stderr, "%% No valid brokers specified\n");


    //Redirect the rd_kafka_poll() queue to the consumer_poll() queue
    rd_kafka_poll_set_consumer(rk);

    //Create a Topic+Partition storage space (list/vector)
    topics_list = rd_kafka_topic_partition_list_new(1);
    //Add Topic+Partition to list
    rd_kafka_topic_partition_list_add(topics_list, topic,-1);//The last one is the subscription partition
    //Open the consumer subscription, the matching topic will be added to the subscription list
    //Rd_kafka_assign //Assignment method
    //Rd_kafka_subscribe //Subscription method

    if ((err = rd_kafka_subscribe(rk, topics_list))){
        fprintf(stderr, "%% Failed to start consuming topics: %s\n", rd_kafka_err2str(err));
        return FALSE;
    }
    /*
    if ((err = rd_kafka_assign(rk, topics_list))){
        fprintf(stderr, "%% Failed to start consuming topics: %s\n", rd_kafka_err2str(err));
        return FALSE;
    }*/
    return TRUE;
}

bool KafkaConsumer::consume_clear()
{
    run = 1;

    while (run){
        rd_kafka_message_t *rkmessage;
        rkmessage = rd_kafka_consumer_poll(rk, 200);
        if (rkmessage){

            int64_t seek_offset = RD_KAFKA_OFFSET_END;
            err = rd_kafka_seek(rkt, 0, seek_offset,
                                2000);
            if (err)
                printf("Seek failed: %s\n",
                       rd_kafka_err2str(err));

            break;

            msg_consume(rkmessage, NULL);
        }
    }
    return 0;
}

string KafkaConsumer::consume()
{
    run = 1;
    string message;
    while (run){
        rd_kafka_message_t *rkmessage;
        rkmessage = rd_kafka_consumer_poll(rk, 300);
        if (rkmessage){
            if (((rkmessage->payload)!=NULL)&&(rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR))
            {
                message = (char *)rkmessage->payload;
            }
            rd_kafka_message_destroy(rkmessage);
            run = 0;//Take only one at a time
        }
    }

    return message;
}