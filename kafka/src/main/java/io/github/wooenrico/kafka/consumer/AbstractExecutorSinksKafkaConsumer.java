package io.github.wooenrico.kafka.consumer;

import io.github.wooenrico.kafka.KafkaUtil;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;

public abstract class AbstractExecutorSinksKafkaConsumer<K, V> extends AbstractSinksKafkaConsumer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(AbstractExecutorSinksKafkaConsumer.class);
    private final ThreadPoolExecutor threadPoolExecutor;
    private final Scheduler scheduler;

    public AbstractExecutorSinksKafkaConsumer(String name, ExecutorConsumerProperties consumerProperties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, ConsumerRebalanceListener consumerRebalanceListener) {
        super(name, consumerProperties, keyDeserializer, valueDeserializer, consumerRebalanceListener);
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
