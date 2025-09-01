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
    private int sinksCacheSize = 100;

    /**
     * sinks cache clear up
     */
    private Duration sinksSubscribeAwait = Duration.ofSeconds(5);

    private Duration sinksSubscribeExtraAwait = Duration.ofSeconds(1);

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

    public int getSinksCacheSize() {
        return sinksCacheSize;
    }

    public void setSinksCacheSize(int sinksCacheSize) {
        this.sinksCacheSize = sinksCacheSize;
    }

    public Duration getSinksSubscribeAwait() {
        return sinksSubscribeAwait;
    }

    public void setSinksSubscribeAwait(Duration sinksSubscribeAwait) {
        this.sinksSubscribeAwait = sinksSubscribeAwait;
    }

    public Duration getSinksSubscribeExtraAwait() {
        return sinksSubscribeExtraAwait;
    }

    public void setSinksSubscribeExtraAwait(Duration sinksSubscribeExtraAwait) {
        this.sinksSubscribeExtraAwait = sinksSubscribeExtraAwait;
    }


    @Override
    public String toString() {
        return "SenderProperties{" +
                "enabled=" + enabled +
                ", properties=" + properties +
                ", closeTimeout=" + closeTimeout +
                ", queueSize=" + queueSize +
                ", sinksEmitTimeout=" + sinksEmitTimeout +
                ", sinksCacheSize=" + sinksCacheSize +
                ", sinksSubscribeAwait=" + sinksSubscribeAwait +
                ", sinksSubscribeExtraAwait=" + sinksSubscribeExtraAwait +
                '}';
    }

    public void addProperties(String key, Object value) {
        this.properties.put(key, value);
    }

    public void addCommonProperties(Properties commonProperties) {
        this.properties = KafkaUtil.mergeProperties(commonProperties, this.properties);
    }
}
