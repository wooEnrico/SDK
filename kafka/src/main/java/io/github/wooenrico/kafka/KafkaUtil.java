package io.github.wooenrico.kafka;

import io.github.wooenrico.kafka.consumer.ConsumerProperties;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

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
        CustomizableThreadFactory customizableThreadFactory = new CustomizableThreadFactory(name + "-");
        return KafkaUtil.newThreadPoolExecutor(consumerProperties.getExecutor(), customizableThreadFactory);
    }

    public static ThreadPoolExecutor newSingleThreadPoolExecutor(String name) {
        CustomizableThreadFactory customizableThreadFactory = new CustomizableThreadFactory(name + "-");
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), customizableThreadFactory);
    }
}
