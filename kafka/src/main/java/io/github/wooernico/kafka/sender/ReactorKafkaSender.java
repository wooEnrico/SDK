package io.github.wooernico.kafka.sender;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.*;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public abstract class ReactorKafkaSender<K, V, T> implements Disposable {

    private static final Logger log = LoggerFactory.getLogger(DefaultKafkaProducer.class);

    protected final SenderProperties properties;
    protected final Serializer<K> keySerializer;
    protected final Serializer<V> valueSerializer;

    private final ConcurrentHashMap<Thread, SinksSendToKafkaSubscriber<K, V, T>> subscribeMap = new ConcurrentHashMap<>(512);
    private final reactor.kafka.sender.KafkaSender<K, V> kafkaSender;
    private final Consumer<SenderResult<T>> senderResultConsumer;

    public ReactorKafkaSender(SenderProperties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(properties, keySerializer, valueSerializer, null);
    }

    public ReactorKafkaSender(SenderProperties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer, Consumer<SenderResult<T>> senderResultConsumer) {
        this.properties = properties;
        this.senderResultConsumer = senderResultConsumer;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        this.kafkaSender = this.createKafkaSender(this.properties, this.keySerializer, this.valueSerializer);
    }


    @Override
    public void dispose() {
        this.subscribeMap.forEach((key, value) -> value.dispose());
    }

    /**
     * 提交队列失败，直接写入
     *
     * @param producerRecord 生产者记录
     * @param object         返回数据
     * @return 提交结果
     */
    public Mono<Void> send(ProducerRecord<K, V> producerRecord, T object) {

        SenderRecord<K, V, T> senderRecord = SenderRecord.create(producerRecord, object);

        Sinks.EmitResult emitResult = this.emitToSinks(senderRecord);

        if (emitResult.isSuccess()) {
            return Mono.empty();
        }

        log.warn("reactor kafka sinks emit fail for {}", emitResult);

        return Mono.defer(() -> {
            this.send(senderRecord).subscribe(senderResult -> {
                if (this.senderResultConsumer != null) {
                    this.senderResultConsumer.accept(senderResult);
                }
            });
            return Mono.empty();
        });
    }

    /**
     * kafka 直接写入
     *
     * @param senderRecord 发送记录
     * @return 发送结果
     */
    public Flux<SenderResult<T>> send(SenderRecord<K, V, T> senderRecord) {
        return this.send(Mono.just(senderRecord));
    }

    /**
     * kafka 直接写入
     *
     * @param senderRecord 发送记录
     * @return 发送结果
     */
    public Flux<SenderResult<T>> send(Publisher<SenderRecord<K, V, T>> senderRecord) {
        return this.kafkaSender.send(senderRecord);
    }


    private Sinks.EmitResult emitToSinks(SenderRecord<K, V, T> senderRecord) {

        SinksSendToKafkaSubscriber<K, V, T> subscriber = getKvtSinksSendToKafkaSubscriber();

        Sinks.EmitResult emitResult = subscriber.tryEmitNext(senderRecord);

        if (emitResult.isSuccess()) {
            return emitResult;
        }

        if (Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER.equals(emitResult)
                || Sinks.EmitResult.FAIL_TERMINATED.equals(emitResult)
                || Sinks.EmitResult.FAIL_CANCELLED.equals(emitResult)) {
            SinksSendToKafkaSubscriber<K, V, T> remove = this.subscribeMap.remove(Thread.currentThread());
            if (remove != null) {
                remove.dispose();
            }
        }

        return emitResult;
    }

    private SinksSendToKafkaSubscriber<K, V, T> getKvtSinksSendToKafkaSubscriber() {
        return this.subscribeMap.computeIfAbsent(Thread.currentThread(), thread -> {
            LinkedBlockingQueue<SenderRecord<K, V, T>> queue = new LinkedBlockingQueue<>(properties.getQueueSize());
            Sinks.Many<SenderRecord<K, V, T>> senderRecordMany = Sinks.many().unicast().onBackpressureBuffer(queue);
            log.info("reactor kafka new sinks for {}, {}", Thread.currentThread().getName(), senderRecordMany.hashCode());
            return new SinksSendToKafkaSubscriber<>(this.kafkaSender, senderRecordMany, this.senderResultConsumer);
        });
    }

    private reactor.kafka.sender.KafkaSender<K, V> createKafkaSender(SenderProperties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        SenderOptions<K, V> senderOptions = SenderOptions.<K, V>create(properties.buildProperties())
                .withKeySerializer(keySerializer)
                .withValueSerializer(valueSerializer)
                .stopOnError(false)
                .closeTimeout(properties.getCloseTimeout());

        return KafkaSender.create(senderOptions);
    }

    static class SinksSendToKafkaSubscriber<K, V, T> implements Disposable {
        private final reactor.kafka.sender.KafkaSender<K, V> kafkaSender;
        private final Sinks.Many<SenderRecord<K, V, T>> sinks;
        private final Consumer<SenderResult<T>> senderResultConsumer;
        private final Disposable subscriber;

        public SinksSendToKafkaSubscriber(KafkaSender<K, V> kafkaSender, Sinks.Many<SenderRecord<K, V, T>> sinks, Consumer<SenderResult<T>> senderResultConsumer) {
            this.kafkaSender = kafkaSender;
            this.sinks = sinks;
            this.senderResultConsumer = senderResultConsumer;

            this.subscriber = this.kafkaSender.send(this.sinks.asFlux()).subscribe(senderResult -> {
                if (this.senderResultConsumer != null) {
                    this.senderResultConsumer.accept(senderResult);
                }
            });
        }

        public Sinks.EmitResult tryEmitNext(SenderRecord<K, V, T> senderRecord) {
            return this.sinks.tryEmitNext(senderRecord);
        }

        @Override
        public void dispose() {
            if (this.subscriber != null && !this.subscriber.isDisposed()) {
                this.subscriber.dispose();
            }
        }
    }
}
