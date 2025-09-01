package io.github.wooenrico.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.publisher.Mono;

import java.util.function.Function;

public class DefaultReactorKafkaConsumer extends AbstractRateLimitExecutorReactorKafkaConsumer<String, String> {

    private final Function<ConsumerRecord<String, String>, Mono<Void>> consumerRecordMonoFunction;

    public DefaultReactorKafkaConsumer(String name, RateLimitExecutorConsumerProperties consumerProperties, Function<ConsumerRecord<String, String>, Mono<Void>> consumer) {
        this(name, consumerProperties, consumer, null);
    }

    public DefaultReactorKafkaConsumer(String name, RateLimitExecutorConsumerProperties consumerProperties, Function<ConsumerRecord<String, String>, Mono<Void>> consumer, ConsumerRebalanceListener consumerRebalanceListener) {
        super(name, consumerProperties, new StringDeserializer(), new StringDeserializer(), consumerRebalanceListener);
        this.consumerRecordMonoFunction = consumer;
    }


    @Override
    protected Mono<Void> rateLimitHandle(ConsumerRecord<String, String> record) {
        return this.consumerRecordMonoFunction.apply(record);
    }
}
