import org.apache.kafka.clients.producer.*;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Operators;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;


public class CustomReactorSenderIT extends AbstractKafkaContainerTest {

    private static final Logger log = LoggerFactory.getLogger(CustomReactorSenderIT.class);

    @Test
    public void test() throws InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(100);

        KafkaProducer<String, String> stringStringKafkaProducer = new KafkaProducer<>(this.senderProperties.getProperties());

        MySender<String, String> stringStringMySender = new MySender<>(stringStringKafkaProducer);

        stringStringMySender.send(
                        Flux.range(0, 100)
                                .map(integerFlux -> new ProducerRecord<>("test", integerFlux + ""))
                )
                .doOnNext(callbackResult -> {
                    countDownLatch.countDown();
                    if (callbackResult.getException() != null) {
                        log.error("send error", callbackResult.getException());
                    } else {
                        log.info("send success {}, {}", callbackResult.getMetadata().topic(), callbackResult.getMetadata().offset());
                    }
                })
                .subscribe();

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS), "Timeout waiting for sends");
    }

    private static class MySender<K, V> {
        private final KafkaProducer<K, V> kafkaProducer;

        public MySender(KafkaProducer<K, V> kafkaProducer) {
            this.kafkaProducer = kafkaProducer;
        }

        Flux<CallbackResult<K, V>> send(Flux<ProducerRecord<K, V>> producerRecordFlux) {
            return producerRecordFlux.as(flux -> new MyFluxOperator<K, V>(flux, kafkaProducer));
        }
    }


    private static class MyFluxOperator<K, V> extends FluxOperator<ProducerRecord<K, V>, CallbackResult<K, V>> {
        private final KafkaProducer<K, V> kafkaProducer;

        public MyFluxOperator(Flux<ProducerRecord<K, V>> source, KafkaProducer<K, V> kafkaProducer) {
            super(source);
            this.kafkaProducer = kafkaProducer;
        }

        @Override
        public void subscribe(CoreSubscriber<? super CallbackResult<K, V>> actual) {
            source.subscribe(new KafkaProducerSubscribe<K, V>(actual, kafkaProducer));
        }
    }

    private enum State {
        INIT,
        ACTIVE,
        INBOUND_DONE,
        COMPLETE
    }

    private static class CallbackResult<K, V> {
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

    private static class KafkaProducerSubscribe<K, V> implements CoreSubscriber<ProducerRecord<K, V>> {

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
    }
}
