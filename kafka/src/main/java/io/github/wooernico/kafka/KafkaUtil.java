package io.github.wooernico.kafka;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
}
