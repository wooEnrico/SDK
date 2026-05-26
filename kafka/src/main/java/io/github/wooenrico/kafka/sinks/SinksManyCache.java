package io.github.wooenrico.kafka.sinks;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.collect.Queues;
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

    @Override
    protected CacheLoader<K, Sinks.Many<V>> getCacheLoader() {
        return k -> {
            LinkedBlockingQueue<V> queue = Queues.newLinkedBlockingQueue(this.getBackpressureBuffer());
            Sinks.Many<V> sinks = Sinks.many().unicast().onBackpressureBuffer(queue);

            // 等待下游真正 request 数据，而不仅仅是 subscribe
            CountDownLatch requestLatch = new CountDownLatch(1);
            try {
                Disposable subscribe = this.flatMap(k, sinks.asFlux().doOnRequest(n -> requestLatch.countDown()))
                        .subscribe(r -> this.result(k, r), e -> log.error("SinksManyCache subscriber error for key: {}", k, e));

                // 等待下游发出 request 信号，说明真正准备好消费数据了
                if (!requestLatch.await(this.getSinksSubscribeAwait().toNanos(), TimeUnit.NANOSECONDS)) {
                    log.warn("SinksManyCache request await timeout for key: {}", k);
                }

                this.disposables.put(k, subscribe);
                log.debug("SinksManyCache created for key: {}, sinks: {}", k, sinks.hashCode());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("SinksManyCache interrupted during creation for key: {}", k, e);
            } catch (Exception e) {
                log.error("SinksManyCache error during creation for key: {}", k, e);
            }

            return sinks;
        };
    }

    @Override
    protected RemovalListener<K, Sinks.Many<V>> getRemovalListener() {
        return (key, value, cause) -> {
            if (value != null) {
                value.tryEmitComplete();
            }
            Disposable removed = disposables.remove(key);
            if (removed != null && !removed.isDisposed()) {
                removed.dispose();
            }
            log.debug("SinksManyCache removalListener for key: {}, cause: {}", key, cause);
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
     * 获取订阅等待时间
     *
     * @return 订阅等待时间
     */
    protected Duration getSinksSubscribeAwait() {
        return Duration.ofSeconds(3);
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
            this.invalidate(key);
            if (!disposable.isDisposed()) {
                disposable.dispose();
            }
        });
        this.disposables.clear();
    }
}
