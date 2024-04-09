import io.github.wooernico.kafka.sender.KafkaProducer;
import io.github.wooernico.kafka.sender.ReactorKafkaSender;
import io.github.wooernico.kafka.sender.SenderProperties;
import org.apache.kafka.clients.producer.Callback;
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

    private static SenderProperties getSenderProperties() {
        SenderProperties senderProperties = new SenderProperties();
        senderProperties.addProperties(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        return senderProperties;
    }

    @Ignore
    @org.junit.Test
    public void testReactorSender() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(count);

        SenderProperties senderProperties = getSenderProperties();
        ReactorKafkaSender<String, String, Object> reactorKafkaSender = new ReactorKafkaSender<String, String, Object>(senderProperties, objectSenderResult -> {
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

        CountDownLatch countDownLatch = new CountDownLatch(count);

        SenderProperties senderProperties = getSenderProperties();

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(senderProperties.getProperties());
        kafkaProducer.afterPropertiesSet();
        for (int i = 0; i < count; i++) {
            kafkaProducer.send("test", i + "", new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    countDownLatch.countDown();
                    if (exception != null) {
                        log.error("send error", exception);
                    }
                    log.info("send complete {}", metadata);
                }
            });

        }

        countDownLatch.await();
    }
}
