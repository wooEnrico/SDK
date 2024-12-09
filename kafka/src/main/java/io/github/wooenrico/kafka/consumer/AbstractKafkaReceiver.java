package io.github.wooenrico.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public abstract class AbstractKafkaReceiver<K, V> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AbstractKafkaReceiver.class);

    protected final String name;

    protected final ConsumerProperties consumerProperties;
    protected final Deserializer<K> keyDeserializer;
    protected final Deserializer<V> valueDeserializer;
    protected final Consumer<Collection<ReceiverPartition>> onAssign;
    protected final Consumer<Collection<ReceiverPartition>> onRevoke;

    private final ConcurrentHashMap<KafkaReceiver<K, V>, Disposable> subscribers = new ConcurrentHashMap<>();
    private final AtomicBoolean close = new AtomicBoolean(false);

    public AbstractKafkaReceiver(String name, ConsumerProperties consumerProperties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, Consumer<Collection<ReceiverPartition>> onAssign, Consumer<Collection<ReceiverPartition>> onRevoke) {
        this.name = name;
        this.consumerProperties = consumerProperties;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.onAssign = onAssign != null ? onAssign : partitions -> log.info("assigned partitions : {}", partitions);
        this.onRevoke = onRevoke != null ? onRevoke : partitions -> log.warn("revoked partitions : {}", partitions);
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

        Disposable disposable = kafkaReceiver.receiveAutoAck()
                .doOnError(e -> {
                    log.error("kafka receiver recreate {}", this.name, e);
                    reConsumerRunnable.run();
                })
                .concatMap(consumerRecordFlux -> {
                    return this.handle(consumerRecordFlux).onErrorResume(throwable -> {
                        log.error("kafka receiver handle error", throwable);
                        return Flux.empty();
                    });
                })
                .subscribe();

        this.subscribers.put(kafkaReceiver, disposable);
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

    protected abstract Flux<ConsumerRecord<K, V>> handle(Flux<ConsumerRecord<K, V>> recordFlux);
}
