import io.github.wooernico.kafka.sender.KafkaProducer;
import io.github.wooernico.kafka.sender.ReactorKafkaSender;
import io.github.wooernico.kafka.sender.SenderProperties;
import org.apache.kafka.clients.producer.ProducerConfig;
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

        SenderProperties senderProperties = getSenderProperties();
        ReactorKafkaSender reactorKafkaSender = new ReactorKafkaSender(senderProperties, objectSenderResult -> {
            countDownLatch.countDown();
            if (objectSenderResult.exception() != null) {
                log.error("send error", objectSenderResult.exception());
            }
            log.info("send complete {}", objectSenderResult.recordMetadata());
        });
        reactorKafkaSender.afterPropertiesSet();

        Flux.range(0, count)
                .flatMap(integer -> reactorKafkaSender.send("test", integer + ""))
                .subscribe();

        countDownLatch.await();
    }

    @Ignore
    @org.junit.Test
    public void testSender() throws Exception {
        SenderProperties senderProperties = getSenderProperties();

        KafkaProducer kafkaProducer = new KafkaProducer(senderProperties.getProperties());
        kafkaProducer.afterPropertiesSet();
        for (int i = 0; i < count; i++) {
            RecordMetadata test = kafkaProducer.send("test", i + "").get();
            log.info("{}", test);
        }
    }

    private static SenderProperties getSenderProperties() {
        SenderProperties senderProperties = new SenderProperties();
        senderProperties.addProperties(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        return senderProperties;
    }
}
