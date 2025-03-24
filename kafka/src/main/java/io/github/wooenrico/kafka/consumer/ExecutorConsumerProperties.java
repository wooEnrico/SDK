package io.github.wooenrico.kafka.consumer;

import io.github.wooenrico.kafka.ExecutorConf;

/**
 * 线程池消费者配置
 */
public class ExecutorConsumerProperties extends ConsumerProperties {
    /**
     * 线程池配置
     */
    private ExecutorConf executor = new ExecutorConf();

    public ExecutorConf getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorConf executor) {
        this.executor = executor;
    }
}
