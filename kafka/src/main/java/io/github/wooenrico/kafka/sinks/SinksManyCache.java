package io.github.wooenrico.kafka.sinks;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.collect.Queues;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class SinksManyCache<K, V, R> extends Cache<K, Sinks.Many<V>> implements Disposable {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SinksManyCache.class);
    protected final Map<K, Disposable> disposables = new ConcurrentHashMap<>(this.getCacheMaximumSize());
    private final Map<K, CountDownLatch> latchMap = new ConcurrentHashMap<>(this.getCacheMaximumSize());

    @Override
    protected CacheLoader<K, Sinks.Many<V>> getCacheLoader() {
        return k -> {
            LinkedBlockingQueue<V> queue = Queues.newLinkedBlockingQueue(this.getBackpressureBuffer());
            Sinks.Many<V> sinks = Sinks.many().unicast().onBackpressureBuffer(queue);
            this.preSubscriberCreated(k);
            Disposable subscribe = this.flatMap(k, sinks.asFlux())
                    .doOnSubscribe(subscription -> this.realSubscribe(k, subscription))
                    .subscribe(r -> this.result(k, r));
            try {
                this.onSubscriberCreated(k, subscribe);
            } catch (Exception e) {
                log.error("Error during onSubscriberCreated for key: {}", k, e);
            }
            this.disposables.put(k, subscribe);
            log.info("SinksManyCache created for key : {}, sinks : {}", k, sinks.hashCode());
            return sinks;
        };
    }

    @Override
    protected RemovalListener<K, Sinks.Many<V>> getRemovalListener() {
        return (key, value, cause) -> {
            if (value != null) {
                value.tryEmitComplete();
            }
            Disposable remove = disposables.remove(key);
            if (remove != null && !remove.isDisposed()) {
                remove.dispose();
            }
            log.info("SinksManyCache removalListener called for key : {}, value : {}, cause: {}", key, value != null ? value.hashCode() : null, cause);
        };
    }

    /**
     * 获取背压缓冲区的大小
     *
     * @return 背压缓冲区大小
     */
    protected int getBackpressureBuffer() {
        return 100;
    }

    /**
     * 将订阅者的键和Flux进行映射
     *
     * @param k    订阅者的键
     * @param flux 订阅者的Flux
     * @return 映射后的Flux
     */
    protected abstract Flux<R> flatMap(K k, Flux<V> flux);

    /**
     * 订阅者创建前的操作
     *
     * @param k 订阅者的键
     */
    protected void preSubscriberCreated(K k) {
        CountDownLatch countDownLatch = this.latchMap.computeIfAbsent(k, key -> new CountDownLatch(1));
        log.info("SinksManyCache preSubscriberCreated called for key: {}, latch : {}", k, countDownLatch);
    }

    /**
     * 真实订阅操作
     *
     * @param k            订阅者的键
     * @param subscription 订阅
     */
    protected void realSubscribe(K k, Subscription subscription) {
        CountDownLatch countDownLatch = this.latchMap.get(k);
        if (countDownLatch != null) {
            countDownLatch.countDown();
            log.info("SinksManyCache realSubscribe called for key: {}, subscription: {}, latch : {}", k, subscription, countDownLatch);
        }

    }

    /**
     * 订阅者创建后的操作
     *
     * @param k          订阅者的键
     * @param disposable 订阅者的可丢弃对象
     * @throws Exception 可能抛出的异常
     */
    protected void onSubscriberCreated(K k, Disposable disposable) throws Exception {
        CountDownLatch countDownLatch = this.latchMap.get(k);
        if (countDownLatch != null) {
            try {
                countDownLatch.await(this.getSinksSubscribeAwait().toNanos(), TimeUnit.NANOSECONDS);
                log.info("SinksManyCache onSubscriberCreated called for key: {}, disposable: {}, latch : {}", k, disposable, countDownLatch);
            } catch (Exception e) {
                log.error("Error during onSubscriberCreated for key: {}", k, e);
            } finally {
                this.latchMap.remove(k);
            }
        }
    }

    /**
     * 获取订阅等待时间
     *
     * @return 订阅等待时间
     */
    protected Duration getSinksSubscribeAwait() {
        return Duration.ofSeconds(5);
    }

    /**
     * 结果处理
     *
     * @param k      订阅者的键
     * @param result 处理结果
     */
    protected abstract void result(K k, R result);

    /**
     * 释放资源
     */
    @Override
    public void dispose() {
        this.disposables.forEach((key, disposable) -> {
            if (!disposable.isDisposed()) {
                disposable.dispose();
            }
        });
    }
}
