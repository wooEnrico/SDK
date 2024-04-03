package io.github.wooernico.kafka.consumer;

import io.github.wooernico.kafka.KafkaUtil;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.ReceiverPartition;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class ReactorKafkaReceiver implements InitializingBean, Disposable {

    private static final Logger log = LoggerFactory.getLogger(ReactorKafkaReceiver.class);

    private final String name;

    private final ConsumerProperties consumerProperties;

    private final Function<ConsumerRecord<String, String>, Mono<Void>> consumer;

    private final Map<ThreadPoolExecutor, Disposable> subscribers = new ConcurrentHashMap<>();

    private final AtomicInteger rebalanceCounter = new AtomicInteger(0);

    private Consumer<Collection<ReceiverPartition>> onAssign = partitions -> log.info("assign partitions : {}", partitions);
    private Consumer<Collection<ReceiverPartition>> onRevoke = partitions -> log.warn("revoke partitions : {}", partitions);

    public ReactorKafkaReceiver(String name, ConsumerProperties consumerProperties, Function<ConsumerRecord<String, String>, Mono<Void>> consumer) {
        this.name = name;
        this.consumerProperties = consumerProperties;
        this.consumer = consumer;
    }

    public ReactorKafkaReceiver(String name, ConsumerProperties consumerProperties, Function<ConsumerRecord<String, String>, Mono<Void>> consumer, Consumer<Collection<ReceiverPartition>> onAssign, Consumer<Collection<ReceiverPartition>> onRevoke) {
        this.name = name;
        this.consumerProperties = consumerProperties;
        this.consumer = consumer;
        this.onAssign = onAssign;
        this.onRevoke = onRevoke;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.subscribe(null);
    }

    private void subscribe(ThreadPoolExecutor threadPoolExecutor) {
        if (threadPoolExecutor != null) {
            Disposable remove = this.subscribers.remove(threadPoolExecutor);
            threadPoolExecutor.shutdown();

            if (remove == null) {
                return;
            } else {
                remove.dispose();
            }
        }

        this.reactorKafkaHandler();
    }

    private void reactorKafkaHandler() {
        CustomizableThreadFactory customizableThreadFactory = new CustomizableThreadFactory(this.name + "-" + this.rebalanceCounter.incrementAndGet() + "-");
        ThreadPoolExecutor threadPoolExecutor = KafkaUtil.newThreadPoolExecutor(this.consumerProperties.getExecutor(), customizableThreadFactory);

        Disposable disposable = KafkaUtil.createKafkaReceiver(this.consumerProperties, onAssign, onRevoke).receiveAutoAck().concatMap(r -> r)
                .flatMap(record -> Mono.defer(() -> this.consumer.apply(record)).subscribeOn(Schedulers.fromExecutor(threadPoolExecutor)))
                .onErrorContinue(e -> !(e instanceof CommitFailedException), (e, o) -> log.error("onErrorContinue record : {}", o, e))
                .doOnError(e -> {
                    log.error("commit failed for rebalanced and recreate {}", this.name, e);
                    this.subscribe(threadPoolExecutor);
                }).subscribe();

        this.subscribers.put(threadPoolExecutor, disposable);
    }

    @Override
    public void dispose() {
        this.subscribers.forEach((threadPoolExecutor, disposable) -> {
            disposable.dispose();
            threadPoolExecutor.shutdown();
        });
    }
}
