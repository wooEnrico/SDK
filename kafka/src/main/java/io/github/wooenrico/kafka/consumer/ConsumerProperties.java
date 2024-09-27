package io.github.wooenrico.kafka.consumer;

import io.github.wooenrico.kafka.ExecutorConf;
import io.github.wooenrico.kafka.KafkaUtil;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerProperties {

    private boolean enabled = true;

    /**
     * 处理器名
     */
    private String handlerBeanName;
    private Properties properties = new Properties();
    private Duration pollTimeout = Duration.ofMillis(1000);
    private Duration closeTimeout = Duration.ofMillis(10000);

    /**
     * 并发度，单实例可创建{@link org.apache.kafka.clients.consumer.KafkaConsumer}数
     */
    private int concurrency = 1;

    /**
     * 消费kafka topic
     */
    private List<String> topic;

    /**
     * 线程池配置
     */
    private ExecutorConf executor = new ExecutorConf();

    /**
     * 消费速率
     */
    private Double rate;

    @Override
    public String toString() {
        return "ConsumerProperties{" +
                "enabled=" + enabled +
                ", handlerBeanName='" + handlerBeanName + '\'' +
                ", properties=" + properties +
                ", pollTimeout=" + pollTimeout +
                ", closeTimeout=" + closeTimeout +
                ", concurrency=" + concurrency +
                ", topic=" + topic +
                ", executor=" + executor +
                '}';
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getHandlerBeanName() {
        return handlerBeanName;
    }

    public void setHandlerBeanName(String handlerBeanName) {
        this.handlerBeanName = handlerBeanName;
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

    public ExecutorConf getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorConf executor) {
        this.executor = executor;
    }

    public void addProperties(String key, Object value) {
        this.properties.put(key, value);
    }

    public void addCommonProperties(Properties commonProperties) {
        this.properties = KafkaUtil.mergeProperties(commonProperties, this.properties);
    }

    public Double getRate() {
        return rate;
    }

    public void setRate(Double rate) {
        this.rate = rate;
    }
}
