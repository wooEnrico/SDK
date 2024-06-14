package io.github.wooenrico.kafka.configuration;

import io.github.wooenrico.kafka.consumer.ConsumerProperties;
import io.github.wooenrico.kafka.sender.SenderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(KafkaProperties.class);

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

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("kafka common consumer properties {}", this.commonConsumerProperties);
        this.consumer.forEach((k, v) -> {
            log.info("kafka consumer properties {} ： {}", k, v);
        });
        log.info("kafka common sender properties {}", this.commonSenderProperties);
        this.sender.forEach((k, v) -> {
            log.info("kafka sender properties {} ： {}", k, v);
        });
    }

}
