package io.github.wooernico.kafka.configuration;

import io.github.wooernico.kafka.consumer.ConsumerProperties;
import io.github.wooernico.kafka.sender.SenderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(KafkaProperties.class);

    /**
     * {@link  org.apache.kafka.clients.producer.ProducerConfig}
     */
    private final Properties commonSenderProperties = new Properties();
    /**
     * kafka 消费者配置 {@link org.apache.kafka.clients.consumer.ConsumerConfig}
     */
    private final Properties commonConsumerProperties = new Properties();

    private Map<String, SenderProperties> sender = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private Map<String, ConsumerProperties> consumer = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

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

    public SenderProperties getSenderProperties(String key) {
        return this.sender.get(key);
    }

    public ConsumerProperties getConsumerProperties(String key) {
        return this.consumer.get(key);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.consumer.forEach((k, v) -> {
            v.addCommonProperties(this.commonConsumerProperties);
            log.info("kafka consumer properties {} ： {}", k, v);
        });

        this.sender.forEach((k, v) -> {
            v.addCommonProperties(this.commonSenderProperties);
            log.info("kafka sender properties {} ： {}", k, v);
        });
    }
}
