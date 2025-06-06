package io.github.wooenrico.kafka.consumer;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Deserializer;

public abstract class AbstractRateLimitExecutorKafkaConsumer<K, V> extends AbstractExecutorKafkaConsumer<K, V> {

    private final RateLimiter rateLimiter;

    public AbstractRateLimitExecutorKafkaConsumer(String name, RateLimitExecutorConsumerProperties consumerProperties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, ConsumerRebalanceListener consumerRebalanceListener) {
        super(name, consumerProperties, keyDeserializer, valueDeserializer, consumerRebalanceListener);
        this.rateLimiter = consumerProperties.getRate() == null ? null : RateLimiter.create(consumerProperties.getRate());
    }

    @Override
    protected void executorHandle(ConsumerRecords<K, V> records) {
        if (this.rateLimiter != null) {
            int count = records == null || records.isEmpty() ? 0 : records.count();
            if (count > 0) {
                this.rateLimiter.acquire(count);
            }
        }
        this.rateLimitHandle(records);
    }

    protected abstract void rateLimitHandle(ConsumerRecords<K, V> records);
}
