package io.github.wooernico.kafka.consumer;

import io.github.wooernico.kafka.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

    private final Map<ThreadPoolExecutor, Disposable> subscribers = new ConcurrentHashMap<>();
    private final AtomicInteger rebalanceCounter = new AtomicInteger(0);
    private final AtomicBoolean close = new AtomicBoolean(false);

    public ReactorKafkaReceiver(String name, ConsumerProperties consumerProperties, Function<ConsumerRecord<K, V>, Mono<Void>> consumer, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this(name, consumerProperties, keyDeserializer, valueDeserializer, consumer,
                partitions -> log.info("assigned partitions : {}", partitions),
                partitions -> log.warn("revoked partitions : {}", partitions)
        );
    }

    public ReactorKafkaReceiver(String name, ConsumerProperties consumerProperties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, Function<ConsumerRecord<K, V>, Mono<Void>> consumer, Consumer<Collection<ReceiverPartition>> onAssign, Consumer<Collection<ReceiverPartition>> onRevoke) {
        this.name = name;
        this.consumerProperties = consumerProperties;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.consumer = consumer;
        this.onAssign = onAssign;
        this.onRevoke = onRevoke;

        this.subscribe();
    }

    @Override
    public void close() throws IOException {
        if (!this.close.compareAndSet(false, true)) {
            return;
        }

        this.subscribers.forEach((threadPoolExecutor, disposable) -> {
            disposable.dispose();
            threadPoolExecutor.shutdown();
        });
    }

    private void subscribe(ThreadPoolExecutor threadPoolExecutor) {
        if (threadPoolExecutor != null) {
            Disposable remove = this.subscribers.remove(threadPoolExecutor);
            threadPoolExecutor.shutdown();

            if (remove == null) {
                return;
            } else {
                remove.dispose();
            }
        }

        if (this.close.get()) {
            return;
        }

        this.subscribe();
    }

    private void subscribe() {
        CustomizableThreadFactory customizableThreadFactory = new CustomizableThreadFactory(this.name + "-" + this.rebalanceCounter.incrementAndGet() + "-");
        ThreadPoolExecutor threadPoolExecutor = KafkaUtil.newThreadPoolExecutor(this.consumerProperties.getExecutor(), customizableThreadFactory);
        KafkaReceiver<K, V> kafkaReceiver = this.createKafkaReceiver(this.consumerProperties, this.keyDeserializer, this.valueDeserializer, this.onAssign, this.onRevoke);
        Disposable disposable = kafkaReceiver.receiveAutoAck().concatMap(r -> r)
                .doOnError(e -> {
                    log.error("kafka receiver recreate {}", this.name, e);
                    this.subscribe(threadPoolExecutor);
                })
                .flatMap(record -> Mono.defer(() -> this.consumer.apply(record))
                        .onErrorResume(throwable -> {
                            log.error("onErrorResume record : {}", record, throwable);
                            return Mono.empty();
                        })
                        .subscribeOn(Schedulers.fromExecutor(threadPoolExecutor))
                )
                .subscribe();

        this.subscribers.put(threadPoolExecutor, disposable);
    }

    private reactor.kafka.receiver.KafkaReceiver<K, V> createKafkaReceiver(ConsumerProperties properties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, Consumer<Collection<ReceiverPartition>> onAssign, Consumer<Collection<ReceiverPartition>> onRevoke) {

        ReceiverOptions<K, V> receiverOption = ReceiverOptions.<K, V>create(properties.buildProperties())
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
