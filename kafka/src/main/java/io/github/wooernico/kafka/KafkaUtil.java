package io.github.wooernico.kafka;

import io.github.wooernico.kafka.consumer.ConsumerProperties;
import io.github.wooernico.kafka.sender.SenderProperties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class KafkaUtil {

    public static reactor.kafka.receiver.KafkaReceiver<String, String> createKafkaReceiver(
            ConsumerProperties properties,
            Consumer<Collection<ReceiverPartition>> onAssign,
            Consumer<Collection<ReceiverPartition>> onRevoke) {
        ReceiverOptions<String, String> receiverOption = ReceiverOptions.<String, String>create(properties.getProperties())
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(new StringDeserializer())
                .subscription(properties.getTopic())
                .addAssignListener(partitions -> {
                    if (onAssign != null) {
                        onAssign.accept(partitions);
                    }
                })
                .addRevokeListener(partitions -> {
                    if (onRevoke != null) {
                        onRevoke.accept(partitions);
                    }
                })
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
