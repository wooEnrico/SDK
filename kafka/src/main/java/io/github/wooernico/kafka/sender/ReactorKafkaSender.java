package io.github.wooernico.kafka.sender;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class ReactorKafkaSender<K, V, T> implements InitializingBean, Disposable {

    private static final Logger log = LoggerFactory.getLogger(ReactorKafkaSender.class);

    private final SenderProperties properties;
    private final ConcurrentHashMap<Thread, Disposable> subscribeMap = new ConcurrentHashMap<>(512);
    private ThreadLocal<Sinks.Many<SenderRecord<K, V, T>>> threadLocal;
    private reactor.kafka.sender.KafkaSender<K, V> kafkaSender;
    private Consumer<SenderResult<T>> senderResultConsumer;
    private Scheduler senderResultScheduler = Schedulers.boundedElastic();

    public ReactorKafkaSender(SenderProperties properties) {
        this.properties = properties;
    }

    public ReactorKafkaSender(SenderProperties properties, Consumer<SenderResult<T>> senderResultConsumer) {
        this.properties = properties;
        this.senderResultConsumer = senderResultConsumer;
    }

    public ReactorKafkaSender(SenderProperties properties, Consumer<SenderResult<T>> senderResultConsumer, Scheduler senderResultScheduler) {
        this.properties = properties;
        this.senderResultConsumer = senderResultConsumer;
        this.senderResultScheduler = senderResultScheduler;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.kafkaSender = this.createKafkaSender(this.properties);
        this.threadLocal = ThreadLocal.withInitial(() -> {
            Sinks.Many<SenderRecord<K, V, T>> senderRecordMany = Sinks.many().unicast()
                    .onBackpressureBuffer(new LinkedBlockingQueue<>(this.properties.getQueueSize()));
            log.info("reactor kafka new sinks for {}, {}", Thread.currentThread().getName(), senderRecordMany.hashCode());
            Disposable subscribe = this.send(senderRecordMany.asFlux().doOnSubscribe(subscription -> {
                log.info("reactor kafka subscribe sinks for {}, {}", Thread.currentThread().getName(), senderRecordMany.hashCode());
            })).publishOn(this.senderResultScheduler).flatMap(this::handleSenderResult).subscribe();
            this.subscribeMap.put(Thread.currentThread(), subscribe);
            return senderRecordMany;
        });
    }

    private reactor.kafka.sender.KafkaSender<K, V> createKafkaSender(SenderProperties properties) {

        SenderOptions<K, V> senderOptions = SenderOptions.<K, V>create(properties.getProperties())
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

        return this.send(Flux.just(senderRecord))
                .publishOn(senderResultScheduler)
                .flatMap(this::handleSenderResult)
                .then();
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
    public void dispose() {
        this.subscribeMap.forEach((k, v) -> {
            if (!v.isDisposed()) {
                v.dispose();
            }
        });
    }

    /**
     * 处理
     *
     * @param objectSenderResult
     * @return
     */
    private Mono<Void> handleSenderResult(SenderResult<T> objectSenderResult) {
        if (this.senderResultConsumer != null) {
            this.senderResultConsumer.accept(objectSenderResult);
        }
        return Mono.empty();
    }
}
