package io.github.wooernico.kafka.sender;

import io.github.wooernico.kafka.KafkaUtil;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Properties;

public class SenderProperties {

    /**
     * {@link  org.apache.kafka.clients.producer.ProducerConfig}
     */
    private final Properties defaultProperties = new Properties() {
        {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        }
    };

    private Boolean enabled;

    private Properties properties = new Properties();

    private Duration closeTimeout = Duration.ofMillis(10000);

    private int queueSize = 100;

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    public Properties getProperties() {
        return this.properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public Duration getCloseTimeout() {
        return closeTimeout;
    }

    public void setCloseTimeout(Duration closeTimeout) {
        this.closeTimeout = closeTimeout;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    @Override
    public String toString() {
        return "SenderProperties{" +
                "enabled=" + enabled +
                ", properties=" + properties +
                ", closeTimeout=" + closeTimeout +
                ", queueSize=" + queueSize +
                '}';
    }

    public void addProperties(String key, Object value) {
        this.properties.put(key, value);
    }

    public void addCommonProperties(Properties commonProperties) {
        this.properties = KafkaUtil.mergeProperties(commonProperties, this.properties);
    }

    public Properties buildProperties() {
        return KafkaUtil.mergeProperties(this.defaultProperties, this.properties);
    }
}
