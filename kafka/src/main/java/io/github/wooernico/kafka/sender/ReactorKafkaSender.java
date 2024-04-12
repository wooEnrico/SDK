package io.github.wooernico.kafka.sender;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import reactor.core.Disposable;
import reactor.core.publisher.*;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class ReactorKafkaSender<K, V, T> implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(ReactorKafkaSender.class);

    private final SenderProperties properties;
    private final ConcurrentHashMap<Thread, Disposable> subscribeMap = new ConcurrentHashMap<>(512);
    private final ThreadLocal<Sinks.Many<SenderRecord<K, V, T>>> threadLocal;
    private final reactor.kafka.sender.KafkaSender<K, V> kafkaSender;
    private final Consumer<SenderResult<T>> senderResultConsumer;

    public ReactorKafkaSender(SenderProperties properties) {
        this(properties, null);
    }

    public ReactorKafkaSender(SenderProperties properties, Consumer<SenderResult<T>> senderResultConsumer) {
        this.properties = properties;
        this.senderResultConsumer = senderResultConsumer;
        this.kafkaSender = this.createKafkaSender(this.properties);
        this.threadLocal = ThreadLocal.withInitial(() -> this.getSenderRecordSinks(this.properties));
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("reactor kafka sender init with {}", this.properties);
    }

    private Sinks.Many<SenderRecord<K, V, T>> getSenderRecordSinks(SenderProperties properties) {
        LinkedBlockingQueue<SenderRecord<K, V, T>> queue = new LinkedBlockingQueue<>(properties.getQueueSize());
        Sinks.Many<SenderRecord<K, V, T>> senderRecordMany = Sinks.many().unicast().onBackpressureBuffer(queue);
        log.info("reactor kafka new sinks for {}, {}", Thread.currentThread().getName(), senderRecordMany.hashCode());

        Disposable subscribe = this.send(senderRecordMany.asFlux().doOnSubscribe(subscription -> {
            log.info("reactor kafka subscribe sinks for {}, {}", Thread.currentThread().getName(), senderRecordMany.hashCode());
        })).subscribe(this::handleSenderResult);
        this.subscribeMap.put(Thread.currentThread(), subscribe);

        return senderRecordMany;
    }

    private reactor.kafka.sender.KafkaSender<K, V> createKafkaSender(SenderProperties properties) {
        SenderOptions<K, V> senderOptions = SenderOptions.<K, V>create(properties.getProperties())
                .stopOnError(false)
                .closeTimeout(properties.getCloseTimeout());

        return KafkaSender.create(senderOptions);
    }

    /**
     * 缓冲队列写入
     *
     * @param topic 主题topic
     * @param value 数据
     * @return 队列写入结果
     */
    public Sinks.EmitResult emit(String topic, V value) {
        return this.emit(topic, null, value);
    }

    /**
     * 缓冲队列写入
     *
     * @param topic 主题topic
     * @param key   分区key
     * @param value 数据
     * @return 队列写入结果
     */
    public Sinks.EmitResult emit(String topic, K key, V value) {
        return this.emit(topic, key, value, null);
    }

    /**
     * 缓冲队列写入
     *
     * @param topic  主题topic
     * @param key    分区key
     * @param value  数据
     * @param object 发送记录meta
     * @return 队列写入结果
     */
    public Sinks.EmitResult emit(String topic, K key, V value, T object) {
        SenderRecord<K, V, T> senderRecord = SenderRecord.create(new ProducerRecord<>(topic, key, value), object);
        return this.emitToSinks(senderRecord);
    }

    /**
     * 缓冲队列写入
     *
     * @param topic 主题topic
     * @param value 数据
     * @return Mono
     */
    public Mono<Void> send(String topic, V value) {
        return this.send(topic, null, value);
    }

    /**
     * 缓冲队列写入
     *
     * @param topic 主题topic
     * @param key   分区key
     * @param value 数据
     * @return Mono
     */
    public Mono<Void> send(String topic, K key, V value) {
        return this.send(topic, key, value, null);
    }

    /**
     * 缓冲队列写入
     *
     * @param topic  主题topic
     * @param key    分区key
     * @param value  数据
     * @param object 发送记录meta
     * @return Mono
     */
    public Mono<Void> send(String topic, K key, V value, T object) {

        SenderRecord<K, V, T> senderRecord = SenderRecord.create(new ProducerRecord<K, V>(topic, key, value), object);

        Sinks.EmitResult emitResult = this.emitToSinks(senderRecord);

        if (emitResult.isSuccess()) {
            return Mono.empty();
        }

        log.warn("reactor kafka sinks emit fail for {}", emitResult);

        return Mono.defer(() -> {
            this.send(Mono.just(senderRecord)).subscribe(this::handleSenderResult);
            return Mono.empty();
        });
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

        Sinks.Many<SenderRecord<K, V, T>> sinks = threadLocal.get();

        Sinks.EmitResult emitResult = sinks.tryEmitNext(senderRecord);

        if (emitResult.isSuccess()) {
            return emitResult;
        }

        if (Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER.equals(emitResult)
                || Sinks.EmitResult.FAIL_TERMINATED.equals(emitResult)
                || Sinks.EmitResult.FAIL_CANCELLED.equals(emitResult)) {

            log.warn("reactor kafka remove sinks for {}, {}", emitResult, sinks.hashCode());
            this.threadLocal.remove();
            Disposable disposable = this.subscribeMap.remove(Thread.currentThread());
            if (disposable != null && !disposable.isDisposed()) {
                disposable.dispose();
            }
        }

        return emitResult;
    }

    @Override
    public void destroy() throws Exception {
        this.subscribeMap.entrySet().stream()
                .filter(entry -> !entry.getValue().isDisposed())
                .forEach(entry -> {
                    entry.getValue().dispose();
                });
    }

    /**
     * 处理
     *
     * @param objectSenderResult
     */
    private void handleSenderResult(SenderResult<T> objectSenderResult) {
        if (this.senderResultConsumer != null) {
            this.senderResultConsumer.accept(objectSenderResult);
        }
    }
}
