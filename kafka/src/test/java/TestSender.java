import io.github.wooenrico.kafka.KafkaProperties;
import io.github.wooenrico.kafka.sender.DefaultKafkaProducer;
import io.github.wooenrico.kafka.sender.DefaultReactorKafkaSender;
import io.github.wooenrico.kafka.sender.SenderProperties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.concurrent.CountDownLatch;

public class TestSender {
    private static final Logger log = LoggerFactory.getLogger(TestSender.class);

    private final int count = 100;

    @Ignore
    @org.junit.Test
    public void testReactorSender() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(count);

        SenderProperties senderProperties = KafkaProperties.LOCAL_SENDER;

        DefaultReactorKafkaSender reactorKafkaSender = new DefaultReactorKafkaSender(senderProperties, producerRecordSenderResult -> {
            countDownLatch.countDown();
            if (producerRecordSenderResult.exception() != null) {
                log.error("send error {}", producerRecordSenderResult.correlationMetadata(), producerRecordSenderResult.exception());
            } else {
                log.info("send complete {}", producerRecordSenderResult.correlationMetadata());
            }
        });

        Flux.range(0, count)
                .flatMap(integer -> reactorKafkaSender.send("test", integer + ""))
                .subscribe();

        countDownLatch.await();

        reactorKafkaSender.close();
    }

    @Ignore
    @org.junit.Test
    public void testSender() throws Exception {

        CountDownLatch countDownLatch = new CountDownLatch(count);

        SenderProperties senderProperties = KafkaProperties.LOCAL_SENDER;

        DefaultKafkaProducer kafkaProducer = new DefaultKafkaProducer(senderProperties.getProperties());

        for (int i = 0; i < count; i++) {
            kafkaProducer.send("test", i + "", new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    countDownLatch.countDown();
                    if (exception != null) {
                        log.error("send error", exception);
                    } else {
                        log.info("send complete {}", metadata);
                    }
                }
            });

        }

        countDownLatch.await();

        kafkaProducer.close();
    }
}
