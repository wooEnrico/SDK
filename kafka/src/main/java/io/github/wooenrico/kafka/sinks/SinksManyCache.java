package io.github.wooenrico.kafka.sinks;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.collect.Queues;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class SinksManyCache<K, V, R> extends Cache<K, Sinks.Many<V>> implements Disposable {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SinksManyCache.class);
    protected final Map<K, Disposable> disposables = new ConcurrentHashMap<>(this.getCacheMaximumSize());

    @Override
    protected CacheLoader<K, Sinks.Many<V>> getCacheLoader() {
        return k -> {
            LinkedBlockingQueue<V> queue = Queues.newLinkedBlockingQueue(this.getBackpressureBuffer());
            Sinks.Many<V> sinks = Sinks.many().unicast().onBackpressureBuffer(queue);
            Disposable subscribe = this.getPublisherFunction().apply(sinks.asFlux())
                    .doOnSubscribe(this.onSubscribe())
                    .subscribe(this.subscribe());
            try {
                this.doOnSubscriberCreated(subscribe);
            } catch (Exception e) {
                log.error("Error during onCreateSubscriber for key: {}", k, e);
            }
            this.disposables.put(k, subscribe);
            log.info("Created sinks for {}, {}", k, sinks.hashCode());
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
            log.info("Removed sinks cache for : {}, value : {}, cause: {}", key, value != null ? value.hashCode() : null, cause);
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
     * 获取发布者函数
     *
     * @return 发布者函数
     */
    protected abstract Function<Flux<V>, Flux<R>> getPublisherFunction();

    /**
     * 获取订阅函数
     *
     * @return 订阅函数
     */
    protected abstract Consumer<? super Subscription> onSubscribe();

    /**
     * 获取订阅者函数
     *
     * @return 订阅者函数
     */
    protected abstract Consumer<R> subscribe();


    /**
     * 处理创建时的操作
     *
     * @param disposable 订阅者
     * @throws Exception 如果处理过程中发生异常
     */
    protected abstract void doOnSubscriberCreated(Disposable disposable) throws Exception;

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
