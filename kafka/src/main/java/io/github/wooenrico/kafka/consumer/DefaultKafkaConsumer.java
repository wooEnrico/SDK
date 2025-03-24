package io.github.wooenrico.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.function.Consumer;

public class DefaultKafkaConsumer extends AbstractRateLimitExecutorKafkaConsumer<String, String> {

    private final Consumer<ConsumerRecord<String, String>> consumer;

    public DefaultKafkaConsumer(String name, RateLimitExecutorConsumerProperties consumerProperties, Consumer<ConsumerRecord<String, String>> consumer) {
        this(name, consumerProperties, consumer, null);
    }

    public DefaultKafkaConsumer(String name, RateLimitExecutorConsumerProperties consumerProperties, Consumer<ConsumerRecord<String, String>> consumer, ConsumerRebalanceListener consumerRebalanceListener) {
        super(name, consumerProperties, new StringDeserializer(), new StringDeserializer(), consumerRebalanceListener);
        this.consumer = consumer;
    }

    @Override
    protected void rateLimitHandle(ConsumerRecord<String, String> record) {
        this.consumer.accept(record);
    }
}
