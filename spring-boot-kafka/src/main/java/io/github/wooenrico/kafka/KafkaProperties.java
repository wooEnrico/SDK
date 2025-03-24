package io.github.wooenrico.kafka;

import io.github.wooenrico.kafka.sender.SenderProperties;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

public class KafkaProperties {
    /**
     * kafka 生产者配置 {@link  org.apache.kafka.clients.producer.ProducerConfig}
     */
    private Properties commonSenderProperties = new Properties();
    /**
     * kafka 消费者配置 {@link org.apache.kafka.clients.consumer.ConsumerConfig}
     */
    private Properties commonConsumerProperties = new Properties();

    /**
     * 自定义发送者配置
     */
    private Map<String, SenderProperties> sender = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    /**
     * 自定义消费者配置
     */
    private Map<String, BeanNameRateLimitExecutorConsumerProperties> consumer = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

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

    public Map<String, BeanNameRateLimitExecutorConsumerProperties> getConsumer() {
        return consumer;
    }

    public void setConsumer(Map<String, BeanNameRateLimitExecutorConsumerProperties> consumer) {
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

    public BeanNameRateLimitExecutorConsumerProperties getConsumerProperties(String key) {
        BeanNameRateLimitExecutorConsumerProperties consumerProperties = this.consumer.get(key);
        if (consumerProperties != null) {
            consumerProperties.addCommonProperties(this.commonConsumerProperties);
        }
        return consumerProperties;
    }

}
