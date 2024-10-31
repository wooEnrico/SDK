package io.github.wooenrico.kafka.consumer;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverPartition;

import java.util.Collection;
import java.util.function.Consumer;

public abstract class AbstractRateLimitExecutorKafkaReceiver<K, V> extends AbstractExecutorKafkaReceiver<K, V> {

    private final RateLimiter rateLimiter;

    public AbstractRateLimitExecutorKafkaReceiver(String name, ConsumerProperties consumerProperties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, Consumer<Collection<ReceiverPartition>> onAssign, Consumer<Collection<ReceiverPartition>> onRevoke) {
        super(name, consumerProperties, keyDeserializer, valueDeserializer, onAssign, onRevoke);
        this.rateLimiter = consumerProperties.getRate() == null ? null : RateLimiter.create(consumerProperties.getRate());
    }

    @Override
    protected Mono<Void> executorHandle(ConsumerRecord<K, V> record) {
        if (this.rateLimiter != null) {
            this.rateLimiter.acquire();
        }
        return this.rateLimitHandle(record);
    }

    protected abstract Mono<Void> rateLimitHandle(ConsumerRecord<K, V> record);
}
