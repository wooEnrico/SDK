package io.github.wooenrico.kafka.sinks;


import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public abstract class Cache<K, V> {

    private LoadingCache<K, V> cache;

    /**
     * 构建缓存
     **/
    public void build() {
        this.cache = this.getCaffeine().build(this.getCacheLoader());
    }

    /**
     * 获取缓存
     *
     * @return 缓存
     */
    protected Caffeine<K, V> getCaffeine() {
        return Caffeine.newBuilder()
                .expireAfterAccess(this.getCacheDuration().toMillis(), TimeUnit.MILLISECONDS)
                .maximumSize(this.getCacheMaximumSize())
                .removalListener(this.getRemovalListener());
    }

    /**
     * 获取移除监听器
     *
     * @return 移除监听器
     */
    protected abstract RemovalListener<K, V> getRemovalListener();

    /**
     * 获取缓存的最大大小
     *
     * @return 缓存最大大小
     */
    protected int getCacheMaximumSize() {
        return 200;
    }

    /**
     * 获取缓存的持续时间
     *
     * @return 缓存持续时间
     */
    protected Duration getCacheDuration() {
        return Duration.ofSeconds(60);
    }

    /**
     * 获取缓存加载器
     *
     * @return 缓存加载器
     */
    protected abstract CacheLoader<K, V> getCacheLoader();

    /**
     * 获取缓存
     *
     * @param key 键
     * @return 值
     */
    public V get(K key) {
        return this.cache.get(key);
    }

    /**
     * 移除缓存
     *
     * @param key 键
     */
    public void invalidate(K key) {
        this.cache.invalidate(key);
    }
}
