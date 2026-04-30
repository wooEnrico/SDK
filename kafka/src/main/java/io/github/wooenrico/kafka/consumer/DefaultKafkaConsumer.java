package io.github.wooenrico.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.function.Consumer;

public class DefaultKafkaConsumer extends AbstractExecutorKafkaConsumer<String, String> {

    private final Consumer<ConsumerRecord<String, String>> consumer;

    public DefaultKafkaConsumer(String name, ExecutorConsumerProperties consumerProperties, Consumer<ConsumerRecord<String, String>> consumer) {
        this(name, consumerProperties, consumer, null);
    }

    public DefaultKafkaConsumer(String name, ExecutorConsumerProperties consumerProperties, Consumer<ConsumerRecord<String, String>> consumer,
            ConsumerRebalanceListener consumerRebalanceListener) {
        super(name, consumerProperties, new StringDeserializer(), new StringDeserializer(), consumerRebalanceListener);
        this.consumer = consumer;
    }

    @Override
    protected void executorHandle(ConsumerRecords<String, String> records) {
        if (records == null || records.isEmpty()) {
            return;
        }
        for (ConsumerRecord<String, String> record : records) {
            this.consumer.accept(record);
        }
    }
}
