package io.github.wooenrico.kafka.consumer;

/**
 * 速率限制线程池消费者配置
 */
public class RateLimitExecutorConsumerProperties extends ExecutorConsumerProperties {
    /**
     * 消费速率
     */
    private Double rate;

    public Double getRate() {
        return rate;
    }

    public void setRate(Double rate) {
        this.rate = rate;
    }
}
