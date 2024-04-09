package io.github.wooernico.kafka.configuration;

import io.github.wooernico.kafka.consumer.ConsumerProperties;
import io.github.wooernico.kafka.sender.SenderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;
import java.util.TreeMap;

@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(KafkaProperties.class);

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

    @Override
    public void afterPropertiesSet() throws Exception {
        this.consumer.forEach((k, v) -> {
            log.info("kafka consumer properties {} ： {}", k, v);
        });

        this.sender.forEach((k, v) -> {
            log.info("kafka sender properties {} ： {}", k, v);
        });
    }
}
