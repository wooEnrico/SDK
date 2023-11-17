package io.github.wooernico.kafka.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public interface ReactorKafkaHandler extends IKafkaHandler, Function<ConsumerRecord<String, String>, Mono<Void>> {

}
