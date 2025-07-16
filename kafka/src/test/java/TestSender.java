import io.github.wooenrico.kafka.sender.DefaultKafkaProducer;
import io.github.wooenrico.kafka.sender.DefaultReactorKafkaSender;
import io.github.wooenrico.kafka.sender.SenderProperties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.SenderResult;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class TestSender {
    private static final Logger log = LoggerFactory.getLogger(TestSender.class);

    public static SenderProperties LOCAL_SENDER = new SenderProperties() {
        {
            setEnabled(true);
            addProperties(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            addProperties(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            addProperties(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        }
    };
    @Ignore
    @org.junit.Test
    public void testReactorSender() throws Exception {

        int count = 100;

        CountDownLatch countDownLatch = new CountDownLatch(count);

        // sender properties
        SenderProperties senderProperties = LOCAL_SENDER;

        // result consumer
        Consumer<SenderResult<ProducerRecord<String, String>>> senderResultConsumer = producerRecordSenderResult -> {
            countDownLatch.countDown();
            if (producerRecordSenderResult.exception() != null) {
                log.error("send error {}", producerRecordSenderResult.correlationMetadata(), producerRecordSenderResult.exception());
            } else {
                log.info("send complete {}", producerRecordSenderResult.correlationMetadata());
            }
        };

        // reactor sender
        DefaultReactorKafkaSender reactorKafkaSender = new DefaultReactorKafkaSender(senderProperties, senderResultConsumer);

        Scheduler senderScheduler = Schedulers.newParallel("sender", Runtime.getRuntime().availableProcessors());
        Flux.range(0, count)
                .flatMap(integer -> {
                    return Mono.defer(() -> {
                        return reactorKafkaSender.send("test", integer.toString());
                    }).subscribeOn(senderScheduler);
                })
                .doOnError(throwable -> {
                    log.error("send error", throwable);
                })
                .subscribe();

        countDownLatch.await();

        reactorKafkaSender.close();
    }

    @Ignore
    @org.junit.Test
    public void testSender() throws Exception {

        // sender properties
        SenderProperties senderProperties = LOCAL_SENDER;

        // sender
        try (DefaultKafkaProducer kafkaProducer = new DefaultKafkaProducer(senderProperties.getProperties())) {
            for (int i = 0; i < 100; i++) {
                kafkaProducer.send("test", i + "", (metadata, exception) -> {
                    if (exception != null) {
                        log.error("send error", exception);
                    } else {
                        log.info("send complete {}", metadata);
                    }
                });
            }
        } catch (Exception e) {
            log.error("send error", e);
        }
    }
}
