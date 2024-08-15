package io.github.wooenrico.kafka.consumer;

import com.google.common.util.concurrent.RateLimiter;
import io.github.wooenrico.kafka.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public abstract class KafkaConsumer<K, V> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    protected final String name;
    protected final ConsumerProperties consumerProperties;
    protected final Consumer<ConsumerRecord<K, V>> consumer;
    protected final Deserializer<K> keyDeserializer;
    protected final Deserializer<V> valueDeserializer;

    private final ConcurrentHashMap<org.apache.kafka.clients.consumer.KafkaConsumer<K, V>, CompletableFuture<Void>> consumerMap = new ConcurrentHashMap<>();
    private final AtomicBoolean close = new AtomicBoolean(false);
    private final ThreadPoolExecutor pollExecutor;
    private final ConsumerRebalanceListener consumerRebalanceListener;
    private final ThreadPoolExecutor threadPoolExecutor;
    private final RateLimiter rateLimiter;

    public KafkaConsumer(String name, ConsumerProperties consumerProperties, Consumer<ConsumerRecord<K, V>> consumer, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this(name, consumerProperties, consumer, keyDeserializer, valueDeserializer, null);
    }

    public KafkaConsumer(String name, ConsumerProperties consumerProperties, Consumer<ConsumerRecord<K, V>> consumer, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, ConsumerRebalanceListener consumerRebalanceListener) {
        this.name = name;
        this.consumerProperties = consumerProperties;
        this.consumer = consumer;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.consumerRebalanceListener = consumerRebalanceListener != null ? consumerRebalanceListener : new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.warn("revoked partitions {}", partitions.toString());
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("assigned partitions {}", partitions.toString());
            }

            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {
                log.info("lost partitions {}", partitions.toString());
            }
        };
        this.rateLimiter = consumerProperties.getRate() == null ? null : RateLimiter.create(consumerProperties.getRate());
        this.pollExecutor = KafkaUtil.newSingleThreadPoolExecutor(name + "-poll");
        this.threadPoolExecutor = KafkaUtil.newThreadPoolExecutor(name, consumerProperties);
        this.subscribe();
    }

    @Override
    public void close() {
        if (!this.close.compareAndSet(false, true)) {
            return;
        }
        this.pollExecutor.shutdown();
        this.threadPoolExecutor.shutdown();
    }

    private void subscribe(org.apache.kafka.clients.consumer.KafkaConsumer<K, V> kafkaConsumer) {
        if (kafkaConsumer != null) {
            CompletableFuture<Void> remove = this.consumerMap.remove(kafkaConsumer);
            if (remove == null) {
                return;
            }
            // kafka close is not thread-safe must call by poll thread
            CompletableFuture.runAsync(kafkaConsumer::close, this.pollExecutor);
        }

        if (this.close.get()) {
            return;
        }

        this.subscribe();
    }

    private void subscribe() {
        org.apache.kafka.clients.consumer.KafkaConsumer<K, V> kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<K, V>(this.consumerProperties.buildProperties(), this.keyDeserializer, this.valueDeserializer);
        kafkaConsumer.subscribe(this.consumerProperties.getTopic(), this.consumerRebalanceListener);
        final Runnable reConsumerRunnable = () -> this.subscribe(kafkaConsumer);

        CompletableFuture<Void> voidCompletableFuture = CompletableFuture.runAsync(() -> {
            this.loopPoll(kafkaConsumer, threadPoolExecutor);
        }, this.pollExecutor).whenComplete((unused, throwable) -> {
            log.error("kafka consumer recreate {}", this.name, throwable);
            reConsumerRunnable.run();
        });

        this.consumerMap.put(kafkaConsumer, voidCompletableFuture);
    }

    private void loopPoll(org.apache.kafka.clients.consumer.KafkaConsumer<K, V> kafkaConsumer, ThreadPoolExecutor threadPoolExecutor) {
        while (!this.close.get()) {
            ConsumerRecords<K, V> records = kafkaConsumer.poll(this.consumerProperties.getPollTimeout());
            for (ConsumerRecord<K, V> record : records) {
                threadPoolExecutor.execute(() -> handleWithRateLimiter(record));
            }
        }
        kafkaConsumer.close();
    }

    private void handleWithRateLimiter(ConsumerRecord<K, V> record) {
        try {
            if (this.rateLimiter != null) {
                this.rateLimiter.acquire();
            }
            this.consumer.accept(record);
        } catch (Exception e) {
            log.error("handle error record : {}", record, e);
        }
    }
}
