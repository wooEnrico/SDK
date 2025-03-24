package io.github.wooenrico.kafka;

import io.github.wooenrico.kafka.consumer.RateLimitExecutorConsumerProperties;


/**
 * 指定handler bean name的速率限制线程池消费者配置
 */
public class BeanNameRateLimitExecutorConsumerProperties extends RateLimitExecutorConsumerProperties {
    /**
     * spring bean 处理器名
     */
    private String handlerBeanName;

    public String getHandlerBeanName() {
        return handlerBeanName;
    }

    public void setHandlerBeanName(String handlerBeanName) {
        this.handlerBeanName = handlerBeanName;
    }
}
