package io.github.wooenrico.kafka.consumer;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

public abstract class AbstractRateLimitExecutorKafkaConsumer<K, V> extends AbstractExecutorKafkaConsumer<K, V> {

    private final RateLimiter rateLimiter;

    public AbstractRateLimitExecutorKafkaConsumer(String name, ConsumerProperties consumerProperties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, ConsumerRebalanceListener consumerRebalanceListener) {
        super(name, consumerProperties, keyDeserializer, valueDeserializer, consumerRebalanceListener);
        this.rateLimiter = consumerProperties.getRate() == null ? null : RateLimiter.create(consumerProperties.getRate());
    }

    @Override
    protected void executorHandle(ConsumerRecord<K, V> record) {
        if (this.rateLimiter != null) {
            this.rateLimiter.acquire();
        }
        this.rateLimitHandle(record);
    }

    protected abstract void rateLimitHandle(ConsumerRecord<K, V> record);
}
