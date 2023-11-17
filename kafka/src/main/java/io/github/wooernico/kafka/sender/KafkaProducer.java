package io.github.wooernico.kafka.sender;

import io.github.wooernico.kafka.KafkaUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.InitializingBean;
import reactor.core.Disposable;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducer implements InitializingBean, Disposable {

    private final Properties properties;

    private org.apache.kafka.clients.producer.KafkaProducer<String, String> kafkaProducer;

    public KafkaProducer(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.kafkaProducer = KafkaUtil.createKafkaProducer(this.properties);
    }

    /**
     * @param topic 主题topic
     * @param value 数据
     * @return
     */
    public Future<RecordMetadata> send(String topic, String value) {
        return this.send(topic, null, value);
    }

    /**
     * @param topic 主题topic
     * @param key   分区key
     * @param value 数据
     * @return
     */
    public Future<RecordMetadata> send(String topic, String key, String value) {
        return this.send(topic, null, key, value);
    }

    /**
     * @param topic     主题topic
     * @param partition 分区ID
     * @param key       分区key
     * @param value     数据
     * @return
     */
    public Future<RecordMetadata> send(String topic, Integer partition, String key, String value) {
        return this.send(topic, partition, null, key, value);
    }

    /**
     * @param topic     主题topic
     * @param partition 分区ID
     * @param timestamp 时间戳
     * @param key       分区key
     * @param value     数据
     * @return
     */
    public Future<RecordMetadata> send(String topic, Integer partition, Long timestamp, String key, String value) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, partition, timestamp, key, value);
        return this.kafkaProducer.send(producerRecord);
    }

    @Override
    public void dispose() {
        this.kafkaProducer.close();
    }
}
