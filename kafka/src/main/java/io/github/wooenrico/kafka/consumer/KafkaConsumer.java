package io.github.wooenrico.kafka.consumer;

import io.github.wooenrico.kafka.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public abstract class KafkaConsumer<K, V> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    protected final String name;
    protected final ConsumerProperties consumerProperties;
    protected final Consumer<ConsumerRecord<K, V>> consumer;
    protected final Deserializer<K> keyDeserializer;
    protected final Deserializer<V> valueDeserializer;

    private final Map<ThreadPoolExecutor, org.apache.kafka.clients.consumer.KafkaConsumer<K, V>> subscribers = new ConcurrentHashMap<>();
    private final AtomicInteger rebalanceCounter = new AtomicInteger(0);
    private final AtomicBoolean close = new AtomicBoolean(false);
    private final Executor pollExecutor;
    private final ConsumerRebalanceListener consumerRebalanceListener;

    public KafkaConsumer(String name, ConsumerProperties consumerProperties, Consumer<ConsumerRecord<K, V>> consumer, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this(name, consumerProperties, consumer, keyDeserializer, valueDeserializer, new ConsumerRebalanceListener() {
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
        });
    }

    public KafkaConsumer(String name, ConsumerProperties consumerProperties, Consumer<ConsumerRecord<K, V>> consumer, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, ConsumerRebalanceListener consumerRebalanceListener) {
        this.name = name;
        this.consumerProperties = consumerProperties;
        this.consumer = consumer;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.consumerRebalanceListener = consumerRebalanceListener == null ? new NoOpConsumerRebalanceListener() : consumerRebalanceListener;
        this.pollExecutor = getKafkaPollExecutor(this.name);

        this.subscribe();
    }

    @Override
    public void close() {
        if (!this.close.compareAndSet(false, true)) {
            return;
        }
        this.subscribers.forEach((threadPoolExecutor, kafkaConsumer) -> {
            threadPoolExecutor.shutdown();
        });
    }

    private void subscribe(ThreadPoolExecutor threadPoolExecutor) {
        if (threadPoolExecutor != null) {
            org.apache.kafka.clients.consumer.KafkaConsumer<K, V> remove = this.subscribers.remove(threadPoolExecutor);
            threadPoolExecutor.shutdown();
            if (remove == null) {
                return;
            } else {
                remove.close();
            }
        }
        this.subscribe();
    }


    private void subscribe() {
        CustomizableThreadFactory customizableThreadFactory = new CustomizableThreadFactory(this.name + "-" + this.rebalanceCounter.incrementAndGet() + "-");
        ThreadPoolExecutor threadPoolExecutor = KafkaUtil.newThreadPoolExecutor(this.consumerProperties.getExecutor(), customizableThreadFactory);

        org.apache.kafka.clients.consumer.KafkaConsumer<K, V> kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<K, V>(this.consumerProperties.buildProperties(), this.keyDeserializer, this.valueDeserializer);
        kafkaConsumer.subscribe(this.consumerProperties.getTopic(), consumerRebalanceListener);
        this.subscribers.put(threadPoolExecutor, kafkaConsumer);

        CompletableFuture.runAsync(() -> {
            this.loopPoll(kafkaConsumer, threadPoolExecutor);
        }, this.pollExecutor).exceptionally(throwable -> {
            if (!close.get()) {
                log.error("kafka consumer recreate {}", this.name, throwable);
                this.subscribe(threadPoolExecutor);
            } else {
                log.error("kafka consumer stop {}", this.name, throwable);
            }
            return null;
        });
    }

    private void loopPoll(org.apache.kafka.clients.consumer.KafkaConsumer<K, V> kafkaConsumer, ThreadPoolExecutor threadPoolExecutor) {
        while (!this.close.get()) {
            ConsumerRecords<K, V> records = kafkaConsumer.poll(this.consumerProperties.getPollTimeout());
            for (ConsumerRecord<K, V> record : records) {
                threadPoolExecutor.execute(() -> {
                    try {
                        this.consumer.accept(record);
                    } catch (Exception e) {
                        log.error("handle error record : {}", record, e);
                    }
                });
            }
        }
        kafkaConsumer.close();
    }

    private Executor getKafkaPollExecutor(String name) {
        return Executors.newSingleThreadExecutor(new CustomizableThreadFactory(name + "-poll-"));
    }
}
