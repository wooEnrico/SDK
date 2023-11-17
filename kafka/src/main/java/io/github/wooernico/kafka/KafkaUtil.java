package io.github.wooernico.kafka;

import io.github.wooernico.kafka.consumer.ConsumerProperties;
import io.github.wooernico.kafka.sender.SenderProperties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class KafkaUtil {

    private static final Logger log = LoggerFactory.getLogger(KafkaUtil.class);

    public static reactor.kafka.receiver.KafkaReceiver<String, String> createKafkaReceiver(ConsumerProperties properties) {
        ReceiverOptions<String, String> receiverOption = ReceiverOptions.<String, String>create(properties.getProperties())
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(new StringDeserializer())
                .subscription(properties.getTopic())
                .addAssignListener(partitions -> log.info("assign partitions : {}", partitions))
                .addRevokeListener(partitions -> log.warn("revoke partitions : {}", partitions))
                .pollTimeout(properties.getPollTimeout())
                .closeTimeout(properties.getCloseTimeout());

        return KafkaReceiver.create(receiverOption);
    }

    public static reactor.kafka.sender.KafkaSender<String, String> createKafkaSender(SenderProperties properties) {

        SenderOptions<String, String> senderOptions = SenderOptions.<String, String>create(properties.getProperties())
                .closeTimeout(properties.getCloseTimeout())
                .withKeySerializer(new StringSerializer())
                .withValueSerializer(new StringSerializer());

        return KafkaSender.create(senderOptions);
    }

    public static org.apache.kafka.clients.producer.KafkaProducer<String, String> createKafkaProducer(Properties properties) {
        return new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
    }

    public static org.apache.kafka.clients.consumer.KafkaConsumer<String, String> createKafkaConsumer(Properties properties) {
        return new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
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
