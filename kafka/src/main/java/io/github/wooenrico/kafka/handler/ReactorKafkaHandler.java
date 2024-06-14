package io.github.wooenrico.kafka.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public interface ReactorKafkaHandler<K, V> extends IKafkaHandler, Function<ConsumerRecord<K, V>, Mono<Void>> {

}
