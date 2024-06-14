package io.github.wooenrico.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.function.Consumer;

public class DefaultKafkaConsumer extends KafkaConsumer<String, String> implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(DefaultKafkaConsumer.class);

    public DefaultKafkaConsumer(String name, ConsumerProperties consumerProperties, Consumer<ConsumerRecord<String, String>> consumer) {
        super(name, consumerProperties, consumer, new StringDeserializer(), new StringDeserializer());
    }

    public DefaultKafkaConsumer(String name, ConsumerProperties consumerProperties, Consumer<ConsumerRecord<String, String>> consumer, ConsumerRebalanceListener consumerRebalanceListener) {
        super(name, consumerProperties, consumer, new StringDeserializer(), new StringDeserializer(), consumerRebalanceListener);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("default kafka consumer named [{}] init with {}", this.name, this.consumerProperties);
    }

    @Override
    public void destroy() throws Exception {
        super.close();
    }
}
