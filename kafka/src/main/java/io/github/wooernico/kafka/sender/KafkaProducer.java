package io.github.wooernico.kafka.sender;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.DisposableBean;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducer<K, V> implements InitializingBean, DisposableBean {

    private final Properties properties;

    private org.apache.kafka.clients.producer.KafkaProducer<K, V> kafkaProducer;

    public KafkaProducer(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<K, V>(this.properties);
    }

    /**
     * @param topic 主题topic
     * @param value 数据
     * @return
     */
    public Future<RecordMetadata> send(String topic, V value) {
        return this.send(topic, null, value);
    }

    /**
     * @param topic    主题topic
     * @param value    数据
     * @param callback 回调
     */
    public void send(String topic, V value, Callback callback) {
        this.send(topic, null, value, callback);
    }

    /**
     * @param topic 主题topic
     * @param key   分区key
     * @param value 数据
     * @return
     */
    public Future<RecordMetadata> send(String topic, K key, V value) {
        return this.send(topic, null, key, value);
    }

    /**
     * @param topic    主题topic
     * @param key      分区key
     * @param value    数据
     * @param callback 回调
     */
    public void send(String topic, K key, V value, Callback callback) {
        this.send(topic, null, key, value, callback);
    }

    /**
     * @param topic     主题topic
     * @param partition 分区ID
     * @param key       分区key
     * @param value     数据
     * @return
     */
    public Future<RecordMetadata> send(String topic, Integer partition, K key, V value) {
        return this.send(topic, partition, null, key, value);
    }

    /**
     * @param topic     主题topic
     * @param partition 分区ID
     * @param key       分区key
     * @param value     数据
     * @param callback  回调
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
     * @return
     */
    public Future<RecordMetadata> send(String topic, Integer partition, Long timestamp, K key, V value) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, partition, timestamp, key, value);
        return this.kafkaProducer.send(producerRecord);
    }

    /**
     * @param topic     主题topic
     * @param partition 分区ID
     * @param timestamp 时间戳
     * @param key       分区key
     * @param value     数据
     * @param callback  回调
     */
    public void send(String topic, Integer partition, Long timestamp, K key, V value, Callback callback) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, partition, timestamp, key, value);
        this.kafkaProducer.send(producerRecord, callback);
    }

    @Override
    public void destroy() throws Exception {
        this.kafkaProducer.close();
    }
}
