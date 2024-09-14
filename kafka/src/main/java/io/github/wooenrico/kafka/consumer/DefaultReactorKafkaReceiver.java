package io.github.wooenrico.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverPartition;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;

public class DefaultReactorKafkaReceiver extends ReactorKafkaReceiver<String, String> {

    public DefaultReactorKafkaReceiver(String name, ConsumerProperties consumerProperties, Function<ConsumerRecord<String, String>, Mono<Void>> consumer) {
        super(name, consumerProperties, consumer, new StringDeserializer(), new StringDeserializer());
    }

    public DefaultReactorKafkaReceiver(String name, ConsumerProperties consumerProperties, Function<ConsumerRecord<String, String>, Mono<Void>> consumer, Consumer<Collection<ReceiverPartition>> onAssign, Consumer<Collection<ReceiverPartition>> onRevoke) {
        super(name, consumerProperties, new StringDeserializer(), new StringDeserializer(), consumer, onAssign, onRevoke);
    }
}
