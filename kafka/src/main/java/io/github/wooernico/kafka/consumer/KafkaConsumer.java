package io.github.wooernico.kafka.consumer;

import io.github.wooernico.kafka.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import reactor.core.Disposable;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class KafkaConsumer implements InitializingBean, Disposable {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    private final String name;

    private final ConsumerProperties consumerProperties;

    private final Consumer<ConsumerRecord<String, String>> consumer;

    private final Map<ThreadPoolExecutor, org.apache.kafka.clients.consumer.KafkaConsumer<String, String>> subscribers = new ConcurrentHashMap<>();

    private final AtomicInteger rebalanceCounter = new AtomicInteger(0);

    private final AtomicBoolean close = new AtomicBoolean(false);

    private final Executor pollExecutor;

    public KafkaConsumer(String name, ConsumerProperties consumerProperties, Consumer<ConsumerRecord<String, String>> consumer) {
        this.name = name;
        this.consumerProperties = consumerProperties;
        this.consumer = consumer;
        this.pollExecutor = Executors.newSingleThreadExecutor(new CustomizableThreadFactory(this.name + "-poll-"));
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.subscribe(null);
    }

    private void subscribe(ThreadPoolExecutor threadPoolExecutor) {

        if (threadPoolExecutor != null) {
            org.apache.kafka.clients.consumer.KafkaConsumer<String, String> remove = this.subscribers.remove(threadPoolExecutor);
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
        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer = KafkaUtil.createKafkaConsumer(this.consumerProperties.getProperties());
        kafkaConsumer.subscribe(this.consumerProperties.getTopic(), new RebalancedListener());

        this.subscribers.put(threadPoolExecutor, kafkaConsumer);

        while (!this.close.get()) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(this.consumerProperties.getPollTimeout());
            for (ConsumerRecord<String, String> record : records) {
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
    public void dispose() {
        this.close.set(true);
        this.subscribers.forEach((threadPoolExecutor, kafkaConsumer) -> {
            threadPoolExecutor.shutdown();
            kafkaConsumer.close();
        });
    }


    static class RebalancedListener implements ConsumerRebalanceListener {
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
    }
}
