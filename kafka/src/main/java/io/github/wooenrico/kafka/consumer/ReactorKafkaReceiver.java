package io.github.wooenrico.kafka.consumer;

import com.google.common.util.concurrent.RateLimiter;
import io.github.wooenrico.kafka.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class ReactorKafkaReceiver<K, V> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ReactorKafkaReceiver.class);

    protected final String name;

    protected final ConsumerProperties consumerProperties;
    protected final Deserializer<K> keyDeserializer;
    protected final Deserializer<V> valueDeserializer;
    protected final Consumer<Collection<ReceiverPartition>> onAssign;
    protected final Consumer<Collection<ReceiverPartition>> onRevoke;
    protected final Function<ConsumerRecord<K, V>, Mono<Void>> consumer;

    private final ConcurrentHashMap<KafkaReceiver<K, V>, Disposable> subscribers = new ConcurrentHashMap<>();
    private final AtomicBoolean close = new AtomicBoolean(false);
    private final ThreadPoolExecutor threadPoolExecutor;
    private final RateLimiter rateLimiter;

    public ReactorKafkaReceiver(String name, ConsumerProperties consumerProperties, Function<ConsumerRecord<K, V>, Mono<Void>> consumer, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this(name, consumerProperties, keyDeserializer, valueDeserializer, consumer, null, null);
    }

    public ReactorKafkaReceiver(String name, ConsumerProperties consumerProperties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, Function<ConsumerRecord<K, V>, Mono<Void>> consumer, Consumer<Collection<ReceiverPartition>> onAssign, Consumer<Collection<ReceiverPartition>> onRevoke) {
        this.name = name;
        this.consumerProperties = consumerProperties;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.consumer = consumer;
        this.onAssign = onAssign != null ? onAssign : partitions -> log.info("assigned partitions : {}", partitions);
        this.onRevoke = onRevoke != null ? onRevoke : partitions -> log.warn("revoked partitions : {}", partitions);
        this.threadPoolExecutor = KafkaUtil.newThreadPoolExecutor(name, consumerProperties);
        this.rateLimiter = consumerProperties.getRate() == null ? null : RateLimiter.create(consumerProperties.getRate());
        if (consumerProperties.isEnabled()) {
            this.subscribe();
        }
    }

    @Override
    public void close() {
        if (!this.close.compareAndSet(false, true)) {
            return;
        }
        this.subscribers.forEach((kafkaReceiver, disposable) -> {
            disposable.dispose();
        });
        this.threadPoolExecutor.shutdown();
    }

    private void subscribe(KafkaReceiver<K, V> kafkaReceiver) {
        if (kafkaReceiver != null) {
            Disposable remove = this.subscribers.remove(kafkaReceiver);
            if (remove == null) {
                return;
            }

            remove.dispose();
        }

        if (this.close.get()) {
            return;
        }

        this.subscribe();
    }

    private void subscribe() {
        KafkaReceiver<K, V> kafkaReceiver = this.createKafkaReceiver(this.consumerProperties, this.keyDeserializer, this.valueDeserializer, this.onAssign, this.onRevoke);
        final Runnable reConsumerRunnable = () -> this.subscribe(kafkaReceiver);

        Disposable disposable = kafkaReceiver.receiveAutoAck().concatMap(r -> r)
                .doOnError(e -> {
                    log.error("kafka receiver recreate {}", this.name, e);
                    reConsumerRunnable.run();
                })
                .flatMap(this::executorHandleRecordWithRateLimiter)
                .subscribe();

        this.subscribers.put(kafkaReceiver, disposable);
    }

    private Mono<Void> executorHandleRecordWithRateLimiter(ConsumerRecord<K, V> record) {
        return Mono.defer(() -> {
                    if (this.rateLimiter != null) {
                        this.rateLimiter.acquire();
                    }
                    return this.consumer.apply(record);
                }).onErrorResume(throwable -> {
                    log.error("onErrorResume record : {}", record, throwable);
                    return Mono.empty();
                })
                .subscribeOn(Schedulers.fromExecutor(threadPoolExecutor));
    }

    private reactor.kafka.receiver.KafkaReceiver<K, V> createKafkaReceiver(ConsumerProperties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, Consumer<Collection<ReceiverPartition>> onAssign, Consumer<Collection<ReceiverPartition>> onRevoke) {

        ReceiverOptions<K, V> receiverOption = ReceiverOptions.<K, V>create(properties.getProperties())
                .withKeyDeserializer(keyDeserializer)
                .withValueDeserializer(valueDeserializer)
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
}
