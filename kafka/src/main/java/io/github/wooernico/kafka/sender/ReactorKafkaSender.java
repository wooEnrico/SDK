package io.github.wooernico.kafka.sender;

import io.github.wooernico.kafka.KafkaUtil;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class ReactorKafkaSender implements InitializingBean, Disposable {

    private static final Logger log = LoggerFactory.getLogger(ReactorKafkaSender.class);

    private final SenderProperties properties;

    private final ConcurrentHashMap<Thread, Sinks.Many<SenderRecord<String, String, Object>>> sinksMap = new ConcurrentHashMap<>(512);
    private final ConcurrentHashMap<Thread, Disposable> subscribeMap = new ConcurrentHashMap<>(512);

    private reactor.kafka.sender.KafkaSender<String, String> kafkaSender;

    public ReactorKafkaSender(SenderProperties properties) {
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.kafkaSender = KafkaUtil.createKafkaSender(this.properties);
    }

    /**
     * 缓冲队列写入
     *
     * @param topic 主题topic
     * @param value 数据
     * @return 队列写入结果
     */
    public Sinks.EmitResult emit(String topic, String value) {
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
    public Sinks.EmitResult emit(String topic, String key, String value) {
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
    public Sinks.EmitResult emit(String topic, String key, String value, Object object) {
        SenderRecord<String, String, Object> senderRecord = SenderRecord.create(new ProducerRecord<>(topic, key, value), object);
        return this.emitToSinks(senderRecord);
    }

    /**
     * 缓冲队列写入
     *
     * @param topic 主题topic
     * @param value 数据
     * @return Mono
     */
    public Mono<Void> send(String topic, String value) {
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
    public Mono<Void> send(String topic, String key, String value) {
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
    public Mono<Void> send(String topic, String key, String value, Object object) {

        SenderRecord<String, String, Object> senderRecord = SenderRecord.create(new ProducerRecord<>(topic, key, value), object);

        Sinks.EmitResult emitResult = this.emitToSinks(senderRecord);

        if (emitResult.isSuccess()) {
            return Mono.empty();
        }

        log.warn("reactor kafka sinks emit fail for {}", emitResult);

        return this.send(Flux.just(senderRecord)).then();
    }

    /**
     * kafka 直接写入
     *
     * @param senderRecord 发送记录
     * @return 发送结果
     */
    public Flux<SenderResult<Object>> send(Publisher<SenderRecord<String, String, Object>> senderRecord) {
        return this.kafkaSender.send(senderRecord);
    }


    private Sinks.EmitResult emitToSinks(SenderRecord<String, String, Object> senderRecord) {

        Thread currentThread = Thread.currentThread();

        Sinks.Many<SenderRecord<String, String, Object>> sinks = this.sinksMap.computeIfAbsent(currentThread, thread -> {
            Sinks.Many<SenderRecord<String, String, Object>> senderRecordMany = Sinks.many().unicast()
                    .onBackpressureBuffer(new LinkedBlockingQueue<>(this.properties.getQueueSize()));
            log.info("reactor kafka new sinks for {}, {}", thread.getName(), senderRecordMany.hashCode());

            Disposable subscribe = send(senderRecordMany.asFlux().doOnSubscribe(subscription -> {
                log.info("reactor kafka subscribe sinks for {}, {}", thread.getName(), senderRecordMany.hashCode());
            })).subscribe();

            this.subscribeMap.put(thread, subscribe);

            return senderRecordMany;
        });

        Sinks.EmitResult emitResult = sinks.tryEmitNext(senderRecord);

        if (emitResult.isSuccess()) {
            return emitResult;
        }

        if (Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER.equals(emitResult)
                || Sinks.EmitResult.FAIL_TERMINATED.equals(emitResult)
                || Sinks.EmitResult.FAIL_CANCELLED.equals(emitResult)) {

            Sinks.Many<SenderRecord<String, String, Object>> remove = this.sinksMap.remove(currentThread);

            if (remove != null) {
                log.warn("reactor kafka remove sinks for {}, {}", emitResult, remove.hashCode());
            }

            Disposable disposable = this.subscribeMap.remove(currentThread);

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
}
