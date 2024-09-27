package io.github.wooenrico.kafka.sender;

import io.github.wooenrico.kafka.KafkaUtil;

import java.time.Duration;
import java.util.Properties;

public class SenderProperties {

    private Boolean enabled;

    private Properties properties = new Properties();

    private Duration closeTimeout = Duration.ofMillis(10000);

    private int queueSize = 100;

    /**
     * where there is no data emit for timeout, then remove the sinks
     */
    private Duration sinksEmitTimeout = Duration.ofSeconds(60);

    /**
     * sinks cache size
     */
    private long sinksCacheSize = 100L;

    /**
     * sinks cache clear up
     */
    private Duration sinksSubscribeAwait = Duration.ofSeconds(5);

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

    public Duration getSinksEmitTimeout() {
        return sinksEmitTimeout;
    }

    public void setSinksEmitTimeout(Duration sinksEmitTimeout) {
        this.sinksEmitTimeout = sinksEmitTimeout;
    }

    public long getSinksCacheSize() {
        return sinksCacheSize;
    }

    public void setSinksCacheSize(long sinksCacheSize) {
        this.sinksCacheSize = sinksCacheSize;
    }

    public Duration getSinksSubscribeAwait() {
        return sinksSubscribeAwait;
    }

    public void setSinksSubscribeAwait(Duration sinksSubscribeAwait) {
        this.sinksSubscribeAwait = sinksSubscribeAwait;
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
}
