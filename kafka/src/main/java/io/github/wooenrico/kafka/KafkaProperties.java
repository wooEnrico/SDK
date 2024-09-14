package io.github.wooenrico.kafka;

import io.github.wooenrico.kafka.consumer.ConsumerProperties;
import io.github.wooenrico.kafka.sender.SenderProperties;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

public class KafkaProperties {

    /**
     * {@link  org.apache.kafka.clients.producer.ProducerConfig}
     */
    private Properties commonSenderProperties = new Properties();
    /**
     * kafka 消费者配置 {@link org.apache.kafka.clients.consumer.ConsumerConfig}
     */
    private Properties commonConsumerProperties = new Properties();

    private Map<String, SenderProperties> sender = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private Map<String, ConsumerProperties> consumer = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public Properties getCommonSenderProperties() {
        return commonSenderProperties;
    }

    public void setCommonSenderProperties(Properties commonSenderProperties) {
        this.commonSenderProperties = commonSenderProperties;
    }

    public Properties getCommonConsumerProperties() {
        return commonConsumerProperties;
    }

    public void setCommonConsumerProperties(Properties commonConsumerProperties) {
        this.commonConsumerProperties = commonConsumerProperties;
    }

    public Map<String, SenderProperties> getSender() {
        return sender;
    }

    public void setSender(Map<String, SenderProperties> sender) {
        this.sender = sender;
    }

    public Map<String, ConsumerProperties> getConsumer() {
        return consumer;
    }

    public void setConsumer(Map<String, ConsumerProperties> consumer) {
        this.consumer = consumer;
    }

    public Set<String> getSenderSet() {
        return sender.keySet();
    }

    public SenderProperties getSenderProperties(String key) {
        SenderProperties senderProperties = this.sender.get(key);
        if (senderProperties != null) {
            senderProperties.addCommonProperties(this.commonSenderProperties);
        }
        return senderProperties;
    }

    public Set<String> getConsumerSet() {
        return consumer.keySet();
    }

    public ConsumerProperties getConsumerProperties(String key) {
        ConsumerProperties consumerProperties = this.consumer.get(key);
        if (consumerProperties != null) {
            consumerProperties.addCommonProperties(this.commonConsumerProperties);
        }
        return consumerProperties;
    }

}
