package io.github.wooenrico.kafka.consumer;

import io.github.wooenrico.kafka.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.concurrent.ThreadPoolExecutor;

public abstract class AbstractExecutorKafkaConsumer<K, V> extends AbstractKafkaConsumer<K, V> {

    private final ThreadPoolExecutor threadPoolExecutor;

    public AbstractExecutorKafkaConsumer(String name, ConsumerProperties consumerProperties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, ConsumerRebalanceListener consumerRebalanceListener) {
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
            this.threadPoolExecutor.execute(() -> this.executorHandle(record));
        }
    }

    protected abstract void executorHandle(ConsumerRecord<K, V> record);
}
