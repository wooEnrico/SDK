package io.github.wooenrico.kafka.sender;

import io.github.wooenrico.kafka.sinks.SinksManyCache;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Duration;
import java.util.function.Consumer;

public class ReactorKafkaSenderSinksManyCache<K, V, T> extends SinksManyCache<Thread, SenderRecord<K, V, T>, SenderResult<T>> {
    private final reactor.kafka.sender.KafkaSender<K, V> kafkaSender;
    private final Consumer<SenderResult<T>> senderResultConsumer;
    private final Duration sinksSubscribeAwait;
    private final Duration cacheDuration;
    private final int cacheMaximumSize;
    private final int backpressureBuffer;

    public ReactorKafkaSenderSinksManyCache(KafkaSender<K, V> kafkaSender, Consumer<SenderResult<T>> senderResultConsumer, Duration sinksSubscribeAwait, Duration cacheDuration, int cacheMaximumSize, int backpressureBuffer) {
        this.kafkaSender = kafkaSender;
        this.senderResultConsumer = senderResultConsumer;
        this.sinksSubscribeAwait = sinksSubscribeAwait;
        this.cacheDuration = cacheDuration;
        this.cacheMaximumSize = cacheMaximumSize;
        this.backpressureBuffer = backpressureBuffer;
    }

    @Override
    protected Flux<SenderResult<T>> flatMap(Thread thread, Flux<SenderRecord<K, V, T>> flux) {
        return this.kafkaSender.send(flux);
    }

    @Override
    protected void result(Thread thread, SenderResult<T> result) {
        this.senderResultConsumer.accept(result);
    }

    @Override
    protected Duration getSinksSubscribeAwait() {
        return this.sinksSubscribeAwait;
    }

    @Override
    protected Duration getCacheDuration() {
        return this.cacheDuration;
    }

    @Override
    protected int getCacheMaximumSize() {
        return this.cacheMaximumSize;
    }

    @Override
    protected int getBackpressureBuffer() {
        return this.backpressureBuffer;
    }
}
