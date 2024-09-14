package io.github.wooenrico.kafka;

import io.github.wooenrico.kafka.consumer.ConsumerProperties;

import java.util.Properties;
import java.util.concurrent.*;

public final class KafkaUtil {

    public static Properties mergeProperties(Properties common, Properties specific) {
        Properties result = new Properties();
        if (common != null) {
            result.putAll(common);
        }

        if (specific != null) {
            result.putAll(specific);
        }

        return result;
    }

    public static ThreadPoolExecutor newThreadPoolExecutor(ExecutorConf executorConf, ThreadFactory threadFactory) {
        return new ThreadPoolExecutor(
                executorConf.getCorePoolSize(),
                executorConf.getMaximumPoolSize(),
                executorConf.getKeepAliveTimeMill(),
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(executorConf.getQueueSize()),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ThreadPoolExecutor newThreadPoolExecutor(String name, ConsumerProperties consumerProperties) {
        NamedThreadFactory namedThreadFactory = new NamedThreadFactory(name);
        return KafkaUtil.newThreadPoolExecutor(consumerProperties.getExecutor(), namedThreadFactory);
    }

    public static ThreadPoolExecutor newSingleThreadPoolExecutor(String name) {
        NamedThreadFactory namedThreadFactory = new NamedThreadFactory(name);
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), namedThreadFactory);
    }
}
