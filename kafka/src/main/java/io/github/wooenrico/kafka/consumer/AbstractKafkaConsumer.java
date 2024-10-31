package io.github.wooenrico.kafka.consumer;

import io.github.wooenrico.kafka.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


public abstract class AbstractKafkaConsumer<K, V> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AbstractKafkaConsumer.class);

    protected final String name;
    protected final ConsumerProperties consumerProperties;
    protected final Deserializer<K> keyDeserializer;
    protected final Deserializer<V> valueDeserializer;

    private final ConcurrentHashMap<org.apache.kafka.clients.consumer.KafkaConsumer<K, V>, CompletableFuture<Void>> consumerMap = new ConcurrentHashMap<>();
    private final AtomicBoolean close = new AtomicBoolean(false);
    private final ThreadPoolExecutor pollExecutor;
    private final ConsumerRebalanceListener consumerRebalanceListener;

    public AbstractKafkaConsumer(String name, ConsumerProperties consumerProperties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, ConsumerRebalanceListener consumerRebalanceListener) {
        this.name = name;
        this.consumerProperties = consumerProperties;
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
        this.pollExecutor = KafkaUtil.newSingleThreadPoolExecutor(name + "-poll");

        if (consumerProperties.isEnabled()) {
            this.subscribe();
        }
    }

    @Override
    public void close() {
        if (!this.close.compareAndSet(false, true)) {
            return;
        }
        this.consumerMap.forEach((key, value) -> {
            // kafka close is not thread-safe must call by poll thread
            CompletableFuture.runAsync(key::close, this.pollExecutor).join();
        });
        this.pollExecutor.shutdown();
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
        org.apache.kafka.clients.consumer.KafkaConsumer<K, V> kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<K, V>(this.consumerProperties.getProperties(), this.keyDeserializer, this.valueDeserializer);
        kafkaConsumer.subscribe(this.consumerProperties.getTopic(), this.consumerRebalanceListener);
        Runnable loopPoll = () -> this.loopPoll(kafkaConsumer);

        CompletableFuture<Void> voidCompletableFuture = CompletableFuture.runAsync(loopPoll, this.pollExecutor)
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        log.error("kafka consumer recreate {}", this.name, throwable);
                        this.subscribe(kafkaConsumer);
                    }
                });

        this.consumerMap.put(kafkaConsumer, voidCompletableFuture);
    }

    private void loopPoll(org.apache.kafka.clients.consumer.KafkaConsumer<K, V> kafkaConsumer) {
        while (!this.close.get()) {
            ConsumerRecords<K, V> records = kafkaConsumer.poll(this.consumerProperties.getPollTimeout());
            if (records == null || records.isEmpty()) {
                continue;
            }
            this.handle(records);
        }
        kafkaConsumer.close();
    }

    protected abstract void handle(ConsumerRecords<K, V> records);
}
