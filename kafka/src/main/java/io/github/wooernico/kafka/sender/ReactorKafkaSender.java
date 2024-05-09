package io.github.wooernico.kafka.sender;

import com.github.benmanes.caffeine.cache.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.*;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public abstract class ReactorKafkaSender<K, V, T> implements Disposable {

    private static final Logger log = LoggerFactory.getLogger(ReactorKafkaSender.class);

    protected final SenderProperties properties;
    protected final Serializer<K> keySerializer;
    protected final Serializer<V> valueSerializer;

    private final LoadingCache<Thread, SinksSendToKafkaSubscriber<K, V, T>> cache;
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

        this.cache = this.getLoadingCache();
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

    private LoadingCache<Thread, SinksSendToKafkaSubscriber<K, V, T>> getLoadingCache() {
        RemovalListener<Thread, SinksSendToKafkaSubscriber<K, V, T>> removalListener = new RemovalListener<Thread, SinksSendToKafkaSubscriber<K, V, T>>() {
            @Override
            public void onRemoval(@Nullable Thread thread, @Nullable SinksSendToKafkaSubscriber<K, V, T> kvtSinksSendToKafkaSubscriber, @NonNull RemovalCause removalCause) {
                log.debug("reactor kafka sinks remove for {}, {}, {}", thread, kvtSinksSendToKafkaSubscriber, removalCause);
                if (kvtSinksSendToKafkaSubscriber != null) {
                    kvtSinksSendToKafkaSubscriber.dispose();
                }
            }
        };

        Caffeine<Thread, SinksSendToKafkaSubscriber<K, V, T>> caffeine = Caffeine.newBuilder()
                .expireAfterAccess(this.properties.getSinksEmitTimeout().toNanos(), TimeUnit.NANOSECONDS)
                .maximumSize(this.properties.getSinksCacheSize())
                .removalListener(removalListener);

        CacheLoader<Thread, SinksSendToKafkaSubscriber<K, V, T>> cacheLoader = new CacheLoader<Thread, SinksSendToKafkaSubscriber<K, V, T>>() {
            @Override
            public SinksSendToKafkaSubscriber<K, V, T> load(@NonNull Thread thread) {
                return newSinksSendToKafkaSubscriber(thread);
            }
        };

        return caffeine.build(cacheLoader);
    }

    @Override
    public void dispose() {
        this.cache.asMap().forEach((key, value) -> value.dispose());
        this.cache.invalidateAll();
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

        return Mono.defer(() -> {
            this.send(senderRecord).subscribe(this.senderResultConsumer);
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
        SinksSendToKafkaSubscriber<K, V, T> subscriber = this.cache.get(Thread.currentThread());
        if (subscriber == null) {
            return Sinks.EmitResult.FAIL_TERMINATED;
        }

        Sinks.EmitResult emitResult = subscriber.tryEmitNext(senderRecord);

        if (emitResult.isSuccess()) {
            return emitResult;
        }

        if (Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER.equals(emitResult)
                || Sinks.EmitResult.FAIL_TERMINATED.equals(emitResult)
                || Sinks.EmitResult.FAIL_CANCELLED.equals(emitResult)) {
            this.cache.invalidate(Thread.currentThread());
            subscriber.dispose();
        }

        return emitResult;
    }

    private SinksSendToKafkaSubscriber<K, V, T> newSinksSendToKafkaSubscriber(Thread thread) {
        if (thread == null) {
            return null;
        }
        LinkedBlockingQueue<SenderRecord<K, V, T>> queue = new LinkedBlockingQueue<>(this.properties.getQueueSize());
        Sinks.Many<SenderRecord<K, V, T>> senderRecordMany = Sinks.many().unicast().onBackpressureBuffer(queue);
        SinksSendToKafkaSubscriber<K, V, T> kvtSinksSendToKafkaSubscriber = new SinksSendToKafkaSubscriber<>(this.kafkaSender, senderRecordMany, this.senderResultConsumer, this.properties.getSinksSubscribeAwait());
        log.debug("reactor kafka new sinks for {}, {}", thread, kvtSinksSendToKafkaSubscriber);
        return kvtSinksSendToKafkaSubscriber;
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
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final Sinks.Many<SenderRecord<K, V, T>> sinks;
        private final Disposable subscriber;

        public SinksSendToKafkaSubscriber(KafkaSender<K, V> kafkaSender, Sinks.Many<SenderRecord<K, V, T>> sinks, Consumer<SenderResult<T>> senderResultConsumer, Duration sinksSubscribeAwait) {
            this.sinks = sinks;

            // CountDownLatch is used to ensure that the subscriber is subscribed before the method returns.
            CountDownLatch countDownLatch = new CountDownLatch(1);
            this.subscriber = kafkaSender.send(this.sinks.asFlux().doOnSubscribe(s -> {
                countDownLatch.countDown();
            })).subscribe(senderResultConsumer);

            try {
                countDownLatch.await(sinksSubscribeAwait.toNanos(), TimeUnit.NANOSECONDS);
            } catch (InterruptedException ignored) {
            }
        }

        public Sinks.EmitResult tryEmitNext(SenderRecord<K, V, T> senderRecord) {
            if (this.closed.get()) {
                return Sinks.EmitResult.FAIL_TERMINATED;
            }
            return this.sinks.tryEmitNext(senderRecord);
        }

        @Override
        public void dispose() {
            if (!this.closed.compareAndSet(false, true)) {
                return;
            }

            Sinks.EmitResult emitResult = this.sinks.tryEmitComplete();
            if (emitResult.isFailure() && this.subscriber != null && !this.subscriber.isDisposed()) {
                this.subscriber.dispose();
            }
        }
    }
}
