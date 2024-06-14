package io.github.wooenrico.kafka.sender;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderResult;

import java.util.function.Consumer;

public abstract class ProducerRecordReactorKafkaSender<K, V> extends ReactorKafkaSender<K, V, ProducerRecord<K, V>> {

    public ProducerRecordReactorKafkaSender(SenderProperties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        super(properties, keySerializer, valueSerializer);
    }

    public ProducerRecordReactorKafkaSender(SenderProperties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer, Consumer<SenderResult<ProducerRecord<K, V>>> senderResultConsumer) {
        super(properties, keySerializer, valueSerializer, senderResultConsumer);
    }

    /**
     * @param topic 主题topic
     * @param value 数据
     * @return Mono
     */
    public Mono<Void> send(String topic, V value) {
        return this.send(topic, null, value);
    }

    /**
     * @param topic 主题topic
     * @param key   分区key
     * @param value 数据
     * @return Mono
     */
    public Mono<Void> send(String topic, K key, V value) {
        return this.send(topic, null, key, value);
    }

    /**
     * @param topic     主题topic
     * @param partition 分区ID
     * @param key       分区key
     * @param value     数据
     * @return Mono
     */
    public Mono<Void> send(String topic, Integer partition, K key, V value) {
        return this.send(topic, partition, null, key, value);
    }

    /**
     * @param topic     主题topic
     * @param partition 分区ID
     * @param timestamp 时间戳
     * @param key       分区key
     * @param value     数据
     * @return Mono
     */
    public Mono<Void> send(String topic, Integer partition, Long timestamp, K key, V value) {
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, timestamp, key, value);
        return super.send(producerRecord, producerRecord);
    }
}
