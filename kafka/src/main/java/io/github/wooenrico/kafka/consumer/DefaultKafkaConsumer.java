package io.github.wooenrico.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.function.Consumer;

public class DefaultKafkaConsumer extends KafkaConsumer<String, String> {

    public DefaultKafkaConsumer(String name, ConsumerProperties consumerProperties, Consumer<ConsumerRecord<String, String>> consumer) {
        super(name, consumerProperties, consumer, new StringDeserializer(), new StringDeserializer());
    }

    public DefaultKafkaConsumer(String name, ConsumerProperties consumerProperties, Consumer<ConsumerRecord<String, String>> consumer, ConsumerRebalanceListener consumerRebalanceListener) {
        super(name, consumerProperties, consumer, new StringDeserializer(), new StringDeserializer(), consumerRebalanceListener);
    }
}
