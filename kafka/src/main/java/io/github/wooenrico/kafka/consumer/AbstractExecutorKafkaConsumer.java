package io.github.wooenrico.kafka.consumer;

import io.github.wooenrico.kafka.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;

public abstract class AbstractExecutorKafkaConsumer<K, V> extends AbstractKafkaConsumer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(AbstractExecutorKafkaConsumer.class);
    private final ThreadPoolExecutor threadPoolExecutor;

    public AbstractExecutorKafkaConsumer(String name, ExecutorConsumerProperties consumerProperties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, ConsumerRebalanceListener consumerRebalanceListener) {
        super(name, consumerProperties, keyDeserializer, valueDeserializer, consumerRebalanceListener);
        this.threadPoolExecutor = KafkaUtil.newThreadPoolExecutor(name, consumerProperties);
    }

    @Override
    public void close() {
        super.close();
        this.threadPoolExecutor.shutdown();
    }

    @Override
    protected void handle(ConsumerRecords<K, V> records) {
        for (ConsumerRecord<K, V> record : records) {
            this.threadPoolExecutor.execute(() -> {
                try {
                    this.executorHandle(record);
                } catch (Exception e) {
                    log.error("executorHandle error", e);
                }
            });
        }
    }

    protected abstract void executorHandle(ConsumerRecord<K, V> record);
}
