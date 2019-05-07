
#include "KafkaProducer.h"
#include <string>
#include <cstring>

int main(int argc, char *argv[]) {

    std::shared_ptr<KafkaProducer> m_pProducerObject = std::make_shared<KafkaProducer>();
    m_pProducerObject->PutBrokers("xx.xx.xx.xx:9092");
    m_pProducerObject->PutTopics({"mauro","test"});
    m_pProducerObject->initKafka();

    std::string xxx = "Hello Kafka!";

    m_pProducerObject->produce(xxx, 0);
    return 0;
}
