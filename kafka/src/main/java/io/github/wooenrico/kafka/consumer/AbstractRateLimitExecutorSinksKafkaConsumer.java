package io.github.wooenrico.kafka.consumer;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import reactor.core.publisher.Mono;

public abstract class AbstractRateLimitExecutorSinksKafkaConsumer<K, V> extends AbstractExecutorSinksKafkaConsumer<K, V> {

    private final RateLimiter rateLimiter;

    public AbstractRateLimitExecutorSinksKafkaConsumer(String name, RateLimitExecutorConsumerProperties consumerProperties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, ConsumerRebalanceListener consumerRebalanceListener) {
        super(name, consumerProperties, keyDeserializer, valueDeserializer, consumerRebalanceListener);
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
