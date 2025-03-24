package io.github.wooenrico.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverPartition;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;

public class DefaultKafkaReceiver extends AbstractRateLimitExecutorKafkaReceiver<String, String> {

    private final Function<ConsumerRecord<String, String>, Mono<Void>> consumerRecordMonoFunction;

    public DefaultKafkaReceiver(String name, RateLimitExecutorConsumerProperties consumerProperties, Function<ConsumerRecord<String, String>, Mono<Void>> consumer) {
        this(name, consumerProperties, consumer, null, null);
    }

    public DefaultKafkaReceiver(String name, RateLimitExecutorConsumerProperties consumerProperties, Function<ConsumerRecord<String, String>, Mono<Void>> consumer, Consumer<Collection<ReceiverPartition>> onAssign, Consumer<Collection<ReceiverPartition>> onRevoke) {
        super(name, consumerProperties, new StringDeserializer(), new StringDeserializer(), onAssign, onRevoke);
        this.consumerRecordMonoFunction = consumer;
    }

    @Override
    protected Mono<Void> rateLimitHandle(ConsumerRecord<String, String> record) {
        return this.consumerRecordMonoFunction.apply(record);
    }
}
