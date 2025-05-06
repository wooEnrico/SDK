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

    /**
     * 自定义topic function
     */
    private String topicFunctionName;

    /**
     * 自定义并发度 function
     */
    private String concurrencyFunctionName;

    public String getHandlerBeanName() {
        return handlerBeanName;
    }

    public void setHandlerBeanName(String handlerBeanName) {
        this.handlerBeanName = handlerBeanName;
    }

    public String getTopicFunctionName() {
        return topicFunctionName;
    }

    public void setTopicFunctionName(String topicFunctionName) {
        this.topicFunctionName = topicFunctionName;
    }

    public String getConcurrencyFunctionName() {
        return concurrencyFunctionName;
    }

    public void setConcurrencyFunctionName(String concurrencyFunctionName) {
        this.concurrencyFunctionName = concurrencyFunctionName;
    }
}
