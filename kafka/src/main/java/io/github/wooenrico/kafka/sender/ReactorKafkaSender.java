package io.github.wooenrico.kafka.sender;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.*;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.io.Closeable;
import java.util.function.Consumer;

public abstract class ReactorKafkaSender<K, V, T> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ReactorKafkaSender.class);

    protected final SenderProperties properties;
    protected final Serializer<K> keySerializer;
    protected final Serializer<V> valueSerializer;

    private final ReactorKafkaSenderSinksManyCache<K, V, T> cache;
    private final reactor.kafka.sender.KafkaSender<K, V> kafkaSender;
    private final Consumer<SenderResult<T>> senderResultConsumer;

    public ReactorKafkaSender(SenderProperties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(properties, keySerializer, valueSerializer, null);
    }

    public ReactorKafkaSender(SenderProperties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer, Consumer<SenderResult<T>> senderResultConsumer) {
        this.properties = properties;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        this.senderResultConsumer = overrideSenderResultConsumer(senderResultConsumer);
        this.kafkaSender = this.createKafkaSender(this.properties, this.keySerializer, this.valueSerializer);

        this.cache = new ReactorKafkaSenderSinksManyCache<>(this.kafkaSender, this.senderResultConsumer,
                this.properties.getSinksSubscribeAwait(),
                this.properties.getSinksSubscribeExtraAwait(),
                this.properties.getSinksEmitTimeout(),
                this.properties.getSinksCacheSize(),
                this.properties.getQueueSize()
        );
        this.cache.build();
    }

    private static <T> Consumer<SenderResult<T>> overrideSenderResultConsumer(Consumer<SenderResult<T>> senderResultConsumer) {
        return new Consumer<SenderResult<T>>() {
            @Override
            public void accept(SenderResult<T> senderResult) {
                try {
                    if (senderResultConsumer != null) {
                        senderResultConsumer.accept(senderResult);
                    }
                } catch (Exception e) {
                    log.error("sender result consume error", e);
                }
            }
        };
    }

    @Override
    public void close() {
        this.cache.dispose();
        this.kafkaSender.close();
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

        log.debug("reactor kafka sinks emit fail for {}", emitResult);

        return this.send(senderRecord)
                .doOnNext(this.senderResultConsumer)
                .then();
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


    /**
     * 提交到 sinks
     *
     * @param senderRecord 发送记录
     * @return 提交结果
     */
    public Sinks.EmitResult emitToSinks(SenderRecord<K, V, T> senderRecord) {
        Sinks.Many<SenderRecord<K, V, T>> senderRecordMany = this.cache.get(Thread.currentThread());
        if (senderRecordMany == null) {
            return Sinks.EmitResult.FAIL_TERMINATED;
        }

        Sinks.EmitResult emitResult = senderRecordMany.tryEmitNext(senderRecord);

        if (emitResult.isSuccess()) {
            return emitResult;
        }

        if (Sinks.EmitResult.FAIL_OVERFLOW.equals(emitResult) || Sinks.EmitResult.FAIL_NON_SERIALIZED.equals(emitResult)) {
            return emitResult;
        }

        // 销毁
        this.cache.invalidate(Thread.currentThread());
        return emitResult;
    }

    private reactor.kafka.sender.KafkaSender<K, V> createKafkaSender(SenderProperties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        SenderOptions<K, V> senderOptions = SenderOptions.<K, V>create(properties.getProperties())
                .withKeySerializer(keySerializer)
                .withValueSerializer(valueSerializer)
                .stopOnError(false)
                .closeTimeout(properties.getCloseTimeout());

        return KafkaSender.create(senderOptions);
    }
}
