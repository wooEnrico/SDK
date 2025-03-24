package io.github.wooenrico.kafka.consumer;

import io.github.wooenrico.kafka.KafkaUtil;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * 消费者配置
 */
public class ConsumerProperties {
    /**
     * 是否启用
     */
    private boolean enabled = true;
    /**
     * 消费者配置
     */
    private Properties properties = new Properties();
    /**
     * 拉取超时时间
     */
    private Duration pollTimeout = Duration.ofMillis(1000);
    /**
     * 关闭超时时间
     */
    private Duration closeTimeout = Duration.ofMillis(10000);

    /**
     * 并发度，单实例可创建{@link org.apache.kafka.clients.consumer.KafkaConsumer}数
     */
    private int concurrency = 1;

    /**
     * 消费kafka topic
     */
    private List<String> topic;

    @Override
    public String toString() {
        return "ConsumerProperties{" +
                "enabled=" + enabled +
                ", properties=" + properties +
                ", pollTimeout=" + pollTimeout +
                ", closeTimeout=" + closeTimeout +
                ", concurrency=" + concurrency +
                ", topic=" + topic +
                '}';
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Properties getProperties() {
        return this.properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public Duration getPollTimeout() {
        return pollTimeout;
    }

    public void setPollTimeout(Duration pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public Duration getCloseTimeout() {
        return closeTimeout;
    }

    public void setCloseTimeout(Duration closeTimeout) {
        this.closeTimeout = closeTimeout;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public List<String> getTopic() {
        return topic;
    }

    public void setTopic(List<String> topic) {
        this.topic = topic;
    }

    public void addProperties(String key, Object value) {
        this.properties.put(key, value);
    }

    public void addCommonProperties(Properties commonProperties) {
        this.properties = KafkaUtil.mergeProperties(commonProperties, this.properties);
    }
}
