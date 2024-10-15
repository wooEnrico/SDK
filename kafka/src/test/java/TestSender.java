import io.github.wooenrico.kafka.KafkaProperties;
import io.github.wooenrico.kafka.sender.DefaultKafkaProducer;
import io.github.wooenrico.kafka.sender.DefaultReactorKafkaSender;
import io.github.wooenrico.kafka.sender.SenderProperties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.SenderResult;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class TestSender {
    private static final Logger log = LoggerFactory.getLogger(TestSender.class);

    @Ignore
    @org.junit.Test
    public void testReactorSender() throws Exception {

        int count = 100;

        CountDownLatch countDownLatch = new CountDownLatch(count);

        // sender properties
        SenderProperties senderProperties = KafkaProperties.LOCAL_SENDER;

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

        Flux.range(0, count)
                .flatMap(integer -> reactorKafkaSender.send("test", integer.toString()))
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
        SenderProperties senderProperties = KafkaProperties.LOCAL_SENDER;

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
