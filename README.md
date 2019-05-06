# kafka-cpp
C++ client (producer and consumer) for Kafka, it leverages librdkafka https://github.com/edenhill/librdkafka

Simple producer

```c++

#include "KafkaProducer.h"
#include <string>
#include <cstring>

int main(int argc, char *argv[]) {

    KafkaProducer* m_pProducerObject;
    m_pProducerObject = new KafkaProducer();
    m_pProducerObject->PutBrokers("xx.xx.xx.xx:9092");
    m_pProducerObject->PutTopics({"mauro","test"});
    m_pProducerObject->initKafka();

    std::string xxx = "Hello Kafka!";

    m_pProducerObject->produce(xxx, 0);
    return 0;
}

```

I'm using [Conan](https://conan.io/) to manage the dependences

## build

```
mkdir build
cd build
conan install .. -s build_type=Debug --install-folder=. --build missing
cmake ..
make
./bin/kafka_test
```

