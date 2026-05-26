package io.github.wooenrico.kafka.consumer;

import com.google.common.collect.Queues;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

public abstract class AbstractSinksKafkaConsumer<K, V> extends AbstractKafkaConsumer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(AbstractSinksKafkaConsumer.class);
    private static final int SINKS_BUFFER_SIZE = Math.max(64, Runtime.getRuntime().availableProcessors() * 8);
    private final Sinks.Many<Flux<ConsumerRecord<K, V>>> many =
            Sinks.many().unicast().onBackpressureBuffer(Queues.newLinkedBlockingQueue(SINKS_BUFFER_SIZE));
    private final Disposable subscribe;

    public AbstractSinksKafkaConsumer(String name, ConsumerProperties consumerProperties, Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer, ConsumerRebalanceListener consumerRebalanceListener) {
        super(name, consumerProperties, keyDeserializer, valueDeserializer, consumerRebalanceListener);
        this.subscribe = this.many.asFlux()
                .concatMap(recordFlux ->
                        this.handle(recordFlux)
                                .onErrorContinue((throwable, obj) -> log.error("Error processing record in flux, continuing: {}", obj, throwable))
                )
                .onErrorContinue((throwable, obj) -> log.error("Error in stream processing, continuing: {}", obj, throwable))
                .doOnError(error -> log.error("Fatal error in stream, this should not happen", error))
                .subscribe();
    }

    @Override
    public void close() {
        super.close();
        Sinks.EmitResult emitResult = this.many.tryEmitComplete();
        if (emitResult.isFailure() && !this.subscribe.isDisposed()) {
            this.subscribe.dispose();
        }
    }

    @Override
    protected void handle(ConsumerRecords<K, V> records) {
        Flux<ConsumerRecord<K, V>> consumerRecordFlux = Flux.fromIterable(records);
        this.many.emitNext(consumerRecordFlux, (signalType, emission) -> {
            if (emission == Sinks.EmitResult.FAIL_OVERFLOW) {
                log.warn("consumer emitNext backpressure, waiting for downstream to catch up");
                LockSupport.parkNanos(100_000_000L);
                return true; // 重试
            }
            log.error("consumer emitNext failed: {}, {}", signalType, emission);
            return emission == Sinks.EmitResult.FAIL_NON_SERIALIZED; // 并发冲突可重试，其他不重试
        });
    }

    protected abstract Flux<ConsumerRecord<K, V>> handle(Flux<ConsumerRecord<K, V>> recordFlux);
}
