package io.github.wooenrico.kafka.sender;

import io.github.wooenrico.kafka.sinks.SinksManyCache;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ReactorKafkaSenderSinksManyCache<K, V, T> extends SinksManyCache<Thread, SenderRecord<K, V, T>, SenderResult<T>> {
    private static final Logger log = LoggerFactory.getLogger(ReactorKafkaSenderSinksManyCache.class);

    private final reactor.kafka.sender.KafkaSender<K, V> kafkaSender;
    private final Consumer<SenderResult<T>> senderResultConsumer;
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
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
    protected Flux<SenderResult<T>> flatMap(Flux<SenderRecord<K, V, T>> flux) {
        return this.kafkaSender.send(flux);
    }

    @Override
    protected void onSubscribe(Subscription subscription) {
        this.countDownLatch.countDown();
    }

    @Override
    protected void subscribe(SenderResult<T> result) {
        senderResultConsumer.accept(result);
    }

    @Override
    protected void doOnSubscriberCreated(Disposable disposable) throws Exception {
        log.info("sinks subscriber created, awaiting subscribe completion");
        try {
            this.countDownLatch.await(this.sinksSubscribeAwait.toNanos(), TimeUnit.NANOSECONDS);
            log.info("sinks subscriber await completed");
        } catch (InterruptedException e) {
            log.error("sinks subscriber await interrupted", e);
        }
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
