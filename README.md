### Kafka CPP

This repo contains two wrappers, one for produce `kafka_p.cc  kafka_p.h` and other to consume `kafka_c.cc  kafka_c.h ` from kafka

Simple example of a producer:


```c++
#include <string>
#include <unistd.h>
#include "kafka_p.h"

int main(int argc, char *argv[]) {
    bool running_ = true;
    KafkaPCB cb;
    std::string brokers = "x.x.x.x:9092";
    std::string topic = "test";
    std::string err_info;
    std::cout << "######### " << brokers << "\n";

    KafkaP kafka_p;
    if (!kafka_p.init(brokers.c_str(), topic.c_str(), err_info, "10000", &cb)) {
        std::cout << "kafka producer init err, " << err_info << "\n";
        exit(1);
    }
    auto test = "Hello";
    while (running_) {
        sleep(0.001);
        if (!kafka_p.produce(test, 5, err_info)) {
            std::cout << "XXX produce err, " << err_info << "\n";
        } else {
            //std::cout << "produce success\n";
        }
    }
    return 0;
}
```
