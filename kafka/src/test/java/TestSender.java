import io.github.wooernico.kafka.sender.KafkaProducer;
import io.github.wooernico.kafka.sender.ReactorKafkaSender;
import io.github.wooernico.kafka.sender.SenderProperties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Duration;

public class TestSender {

    private static final Logger log = LoggerFactory.getLogger(TestSender.class);

    private final int count = 100;

    @Ignore
    @org.junit.Test
    public void testReactorSender() throws Exception {
        SenderProperties senderProperties = getSenderProperties();
        ReactorKafkaSender reactorKafkaSender = new ReactorKafkaSender(senderProperties);
        reactorKafkaSender.afterPropertiesSet();

        Flux.interval(Duration.ofSeconds(1))
                .flatMap(integer -> reactorKafkaSender.send("test", integer + ""))
                .blockLast();
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
