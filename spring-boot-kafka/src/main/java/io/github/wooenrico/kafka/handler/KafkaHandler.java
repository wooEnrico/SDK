package io.github.wooenrico.kafka.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.Consumer;

public interface KafkaHandler<K, V> extends IKafkaHandler, Consumer<ConsumerRecord<K, V>> {
}
