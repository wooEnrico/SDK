package io.github.wooernico.kafka.sender;

import java.time.Duration;
import java.util.Properties;

public class SenderProperties {

    private Boolean enabled;
    /**
     * {@link  org.apache.kafka.clients.producer.ProducerConfig}
     */
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
        return properties;
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
}
