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


public abstract class AbstractReactorKafkaConsumer<K, V> extends AbstractKafkaConsumer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(AbstractReactorKafkaConsumer.class);
    private final Sinks.Many<Flux<ConsumerRecord<K, V>>> many = Sinks.many().unicast().onBackpressureBuffer(Queues.newArrayBlockingQueue(1));
    private final Disposable subscribe;

    public AbstractReactorKafkaConsumer(String name, ConsumerProperties consumerProperties, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, ConsumerRebalanceListener consumerRebalanceListener) {
        super(name, consumerProperties, keyDeserializer, valueDeserializer, consumerRebalanceListener);
        this.subscribe = this.many.asFlux().flatMap(this::handle).subscribe();
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
            log.error("consumer emitNext failed: {}, {}", signalType, emission);
            return emission.isFailure();
        });
    }

    protected abstract Flux<ConsumerRecord<K, V>> handle(Flux<ConsumerRecord<K, V>> recordFlux);
}
