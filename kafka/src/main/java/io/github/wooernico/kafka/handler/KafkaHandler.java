package io.github.wooernico.kafka.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.Consumer;

public interface KafkaHandler extends IKafkaHandler<String, String>, Consumer<ConsumerRecord<String, String>> {
}
