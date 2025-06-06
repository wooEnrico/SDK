package io.github.wooenrico.kafka.handler;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.function.Consumer;

/**
 * Kafka Handler.
 * @param <K>
 * @param <V>
 */
public interface KafkaHandler<K, V> extends IKafkaHandler, Consumer<ConsumerRecords<K, V>> {
}
