package io.github.wooernico.kafka.sender;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

public abstract class KafkaProducer<K, V> implements Closeable {

    protected final Properties properties;
    protected final Serializer<K> keySerializer;
    protected final Serializer<V> valueSerializer;

    private final org.apache.kafka.clients.producer.KafkaProducer<K, V> kafkaProducer;

    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.properties = properties;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(this.properties, this.keySerializer, this.valueSerializer);
    }

    @Override
    public void close() throws IOException {
        this.kafkaProducer.close();
    }

    /**
     * @param topic 主题topic
     * @param value 数据
     * @return 提交结果
     */
    public Future<RecordMetadata> send(String topic, V value) {
        return this.send(topic, null, value);
    }

    /**
     * @param topic 主题topic
     * @param key   分区key
     * @param value 数据
     * @return 提交结果
     */
    public Future<RecordMetadata> send(String topic, K key, V value) {
        return this.send(topic, null, key, value);
    }

    /**
     * @param topic     主题topic
     * @param partition 分区ID
     * @param key       分区key
     * @param value     数据
     * @return 提交结果
     */
    public Future<RecordMetadata> send(String topic, Integer partition, K key, V value) {
        return this.send(topic, partition, null, key, value);
    }

    /**
     * @param topic     主题topic
     * @param partition 分区ID
     * @param timestamp 时间戳
     * @param key       分区key
     * @param value     数据
     * @return 提交结果
     */
    public Future<RecordMetadata> send(String topic, Integer partition, Long timestamp, K key, V value) {
        return this.send(new ProducerRecord<>(topic, partition, timestamp, key, value));
    }

    /**
     * @param producerRecord 生产者记录
     * @return 提交结果
     */
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
        return this.kafkaProducer.send(producerRecord);
    }


    /**
     * @param topic 主题topic
     * @param value 数据
     */
    public void send(String topic, V value, Callback callback) {
        this.send(topic, null, value, callback);
    }

    /**
     * @param topic 主题topic
     * @param key   分区key
     * @param value 数据
     */
    public void send(String topic, K key, V value, Callback callback) {
        this.send(topic, null, key, value, callback);
    }

    /**
     * @param topic     主题topic
     * @param partition 分区ID
     * @param key       分区key
     * @param value     数据
     */
    public void send(String topic, Integer partition, K key, V value, Callback callback) {
        this.send(topic, partition, null, key, value, callback);
    }

    /**
     * @param topic     主题topic
     * @param partition 分区ID
     * @param timestamp 时间戳
     * @param key       分区key
     * @param value     数据
     */
    public void send(String topic, Integer partition, Long timestamp, K key, V value, Callback callback) {
        this.send(new ProducerRecord<>(topic, partition, timestamp, key, value), callback);
    }

    /**
     * @param producerRecord 生产者记录
     * @param callback       回调
     */
    public void send(ProducerRecord<K, V> producerRecord, Callback callback) {
        this.kafkaProducer.send(producerRecord, callback);
    }
}
