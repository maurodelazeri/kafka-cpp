# kafka-cpp
C++ client (producer and consumer) for Kafka, it leverage librdkafka https://github.com/edenhill/librdkafka

Simple producer

```c++

#include "KafkaProducer.h"
#include <string>
#include <cstring>

int main(int argc, char *argv[]) {
    CKafkaProducer kp;

    char topic[] = "test";
    char brokers[] = "xx.xx.xx.xx:9092";
    int partition = 0;

    char str_msg[] = "Hello Kafka!";
    int ret = 0;

    ret = kp.init(topic, brokers, partition);
    if (ret != 0) {
        printf("Error: kp.init(): ret=%d;\n", ret);
        return 0;
    }

    ret = kp.sendMessage(str_msg, strlen(str_msg));
    if (ret != 0) {
        printf("Error: kp.sendMessage(): ret=%d;\n", ret);
        return 0;
    }

    return 0;

}

```
