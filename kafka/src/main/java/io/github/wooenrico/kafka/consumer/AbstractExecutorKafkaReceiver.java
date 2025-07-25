package io.github.wooenrico.kafka.consumer;

import io.github.wooenrico.kafka.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.ReceiverPartition;

import java.util.Collection;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Supplier;

public abstract class AbstractExecutorKafkaReceiver<K, V> extends AbstractKafkaReceiver<K, V> {

    private static final Logger log = LoggerFactory.getLogger(AbstractExecutorKafkaReceiver.class);
    private final ThreadPoolExecutor threadPoolExecutor;
    private final Scheduler scheduler;

    public AbstractExecutorKafkaReceiver(String name, ExecutorConsumerProperties consumerProperties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, Consumer<Collection<ReceiverPartition>> onAssign, Consumer<Collection<ReceiverPartition>> onRevoke) {
        super(name, consumerProperties, keyDeserializer, valueDeserializer, onAssign, onRevoke);
        this.threadPoolExecutor = KafkaUtil.newThreadPoolExecutor(name, consumerProperties);
        this.scheduler = Schedulers.fromExecutor(threadPoolExecutor);
    }

    @Override
    public void close() {
        super.close();
        this.threadPoolExecutor.shutdown();
    }

    @Override
    protected Flux<ConsumerRecord<K, V>> handle(Flux<ConsumerRecord<K, V>> consumerRecordFlux) {
        return consumerRecordFlux.flatMap(record -> {
            return Mono.defer((Supplier<Mono<ConsumerRecord<K, V>>>) () -> this.executorHandle(record)
                    .thenReturn(record)
                    .onErrorResume(throwable -> {
                        log.error("executorHandle error", throwable);
                        return Mono.just(record);
                    })
            ).subscribeOn(this.scheduler);
        });
    }

    protected abstract Mono<Void> executorHandle(ConsumerRecord<K, V> record);
}
