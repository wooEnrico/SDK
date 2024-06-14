import io.github.wooenrico.kafka.sender.SenderProperties;
import org.apache.kafka.clients.producer.*;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Operators;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


public class KafkaProducerSubscribeTest {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerSubscribeTest.class);

    @Test
    @Ignore
    public void test() {
        SenderProperties senderProperties = new SenderProperties();
        senderProperties.addProperties(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

        KafkaProducer<String, String> stringStringKafkaProducer = new KafkaProducer<>(senderProperties.getProperties());

        Flux.range(0, 100)
                .map(integerFlux -> new ProducerRecord<String, String>("test", integerFlux + ""))
                .as(flux -> new FluxOperator<ProducerRecord<String, String>, KafkaProducerSubscribe.CallbackResult<String, String>>(flux) {
                    @Override
                    public void subscribe(CoreSubscriber<? super KafkaProducerSubscribe.CallbackResult<String, String>> actual) {
                        source.subscribe(new KafkaProducerSubscribe<>(actual, stringStringKafkaProducer));
                    }
                })
                .doOnNext(callbackResult -> {
                    if (callbackResult.getException() != null) {
                        log.error("send error", callbackResult.getException());
                    }else {
                        log.info("send success {}, {}", callbackResult.getMetadata().topic(), callbackResult.getMetadata().offset());
                    }
                })
                .blockLast();
    }

}

class KafkaProducerSubscribe<K, V> implements CoreSubscriber<ProducerRecord<K, V>> {

    enum State {
        INIT,
        ACTIVE,
        INBOUND_DONE,
        COMPLETE
    }

    private final KafkaProducer<K, V> kafkaProducer;
    private final CoreSubscriber<? super CallbackResult<K, V>> actual;

    private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
    private final AtomicInteger inflight = new AtomicInteger();

    public KafkaProducerSubscribe(CoreSubscriber<? super CallbackResult<K, V>> actual, KafkaProducer<K, V> kafkaProducer) {
        this.actual = actual;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void onSubscribe(Subscription s) {
        state.set(State.ACTIVE);
        actual.onSubscribe(s);
    }

    @Override
    public void onNext(ProducerRecord<K, V> kvProducerRecord) {
        if (state.get() == State.COMPLETE) {
            Operators.onNextDropped(kvProducerRecord, currentContext());
            return;
        }

        inflight.incrementAndGet();

        Callback callback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                inflight.decrementAndGet();
                actual.onNext(new CallbackResult<>(kvProducerRecord, metadata, exception));
                if (inflight.decrementAndGet() == 0) {
                    tryComplete();
                }
            }
        };

        try {
            kafkaProducer.send(kvProducerRecord, callback);
        } catch (Exception e) {
            callback.onCompletion(null, e);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (state.getAndSet(State.COMPLETE) == State.COMPLETE) {
            Operators.onErrorDropped(t, currentContext());
            return;
        }

        actual.onError(t);
    }

    @Override
    public void onComplete() {
        if (state.compareAndSet(State.ACTIVE, State.INBOUND_DONE)) {
            if (inflight.get() == 0) {
                tryComplete();
            }
        }
    }

    private void tryComplete() {
        if (state.compareAndSet(State.INBOUND_DONE, State.COMPLETE)) {
            actual.onComplete();
        }
    }

    static class CallbackResult<K, V> {
        private final ProducerRecord<K, V> record;
        private final RecordMetadata metadata;
        private final Exception exception;

        public CallbackResult(ProducerRecord<K, V> record, RecordMetadata metadata, Exception exception) {
            this.record = record;
            this.metadata = metadata;
            this.exception = exception;
        }

        public ProducerRecord<K, V> getRecord() {
            return record;
        }

        public RecordMetadata getMetadata() {
            return metadata;
        }

        public Exception getException() {
            return exception;
        }
    }

}
