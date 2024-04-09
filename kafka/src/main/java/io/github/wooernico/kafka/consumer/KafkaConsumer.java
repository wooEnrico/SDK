package io.github.wooernico.kafka.consumer;

import io.github.wooernico.kafka.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class KafkaConsumer<K, V> implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    private final String name;

    private final ConsumerProperties consumerProperties;

    private final Consumer<ConsumerRecord<K, V>> consumer;

    private final Map<ThreadPoolExecutor, org.apache.kafka.clients.consumer.KafkaConsumer<K, V>> subscribers = new ConcurrentHashMap<>();

    private final AtomicInteger rebalanceCounter = new AtomicInteger(0);

    private final AtomicBoolean close = new AtomicBoolean(false);

    private final Executor pollExecutor;

    private ConsumerRebalanceListener consumerRebalanceListener = new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.warn("revoke partitions {}", partitions.toString());
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.info("assign partitions {}", partitions.toString());
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            log.info("lost partitions {}", partitions.toString());
        }
    };

    public KafkaConsumer(String name, ConsumerProperties consumerProperties, Consumer<ConsumerRecord<K, V>> consumer) {
        this.name = name;
        this.consumerProperties = consumerProperties;
        this.consumer = consumer;
        this.pollExecutor = getKafkaPollExecutor(this.name);
    }

    public KafkaConsumer(String name, ConsumerProperties consumerProperties, Consumer<ConsumerRecord<K, V>> consumer, ConsumerRebalanceListener consumerRebalanceListener) {
        this.name = name;
        this.consumerProperties = consumerProperties;
        this.consumer = consumer;
        this.consumerRebalanceListener = consumerRebalanceListener == null ? new NoOpConsumerRebalanceListener() : consumerRebalanceListener;
        this.pollExecutor = getKafkaPollExecutor(this.name);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.subscribe(null);
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

        CompletableFuture.runAsync(this::loopPoll, this.pollExecutor).exceptionally(throwable -> {
            log.error("stop poll and close consumer {}", this.name, throwable);
            if (!close.get()) {
                this.subscribe(threadPoolExecutor);
            }
            return null;
        });
    }

    private void loopPoll() {
        CustomizableThreadFactory customizableThreadFactory = new CustomizableThreadFactory(this.name + "-" + this.rebalanceCounter.incrementAndGet() + "-");
        ThreadPoolExecutor threadPoolExecutor = KafkaUtil.newThreadPoolExecutor(this.consumerProperties.getExecutor(), customizableThreadFactory);

        org.apache.kafka.clients.consumer.KafkaConsumer<K, V> kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<K, V>((this.consumerProperties.getProperties()));
        kafkaConsumer.subscribe(this.consumerProperties.getTopic(), consumerRebalanceListener);

        this.subscribers.put(threadPoolExecutor, kafkaConsumer);

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

    @Override
    public void destroy() throws Exception {
        this.close.set(true);
        this.subscribers.forEach((threadPoolExecutor, kafkaConsumer) -> {
            threadPoolExecutor.shutdown();
        });
    }

    private Executor getKafkaPollExecutor(String name) {
        return Executors.newSingleThreadExecutor(new CustomizableThreadFactory(name + "-poll-"));
    }
}
