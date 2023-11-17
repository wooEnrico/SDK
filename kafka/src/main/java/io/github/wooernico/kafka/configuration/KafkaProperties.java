package io.github.wooernico.kafka.configuration;

import io.github.wooernico.kafka.consumer.ConsumerProperties;
import io.github.wooernico.kafka.sender.SenderProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
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

    private Properties commonSenderProperties = new Properties() {
        {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        }
    };
    private Map<String, SenderProperties> sender = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private Properties commonConsumerProperties = new Properties() {
        {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        }
    };
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

    @Override
    public void afterPropertiesSet() throws Exception {
        this.consumer.forEach((k, v) -> {
            v.setProperties(mergeProperties(this.commonConsumerProperties, v.getProperties()));
            log.info("kafka consumer properties {} ： {}", k, v);
        });

        this.sender.forEach((k, v) -> {
            v.setProperties(mergeProperties(this.commonSenderProperties, v.getProperties()));
            log.info("kafka sender properties {} ： {}", k, v);
        });
    }

    private Properties mergeProperties(Properties common, Properties specific) {
        Properties result = new Properties();
        if (common != null) {
            result.putAll(common);
        }

        if (specific != null) {
            result.putAll(specific);
        }

        return result;
    }
}
