package io.github.wooenrico.test;

import io.github.wooenrico.MainApp;
import io.github.wooenrico.config.TestConfig;
import io.github.wooenrico.handler.MyHandler;
import io.github.wooenrico.handler.MyReactorHandler;
import io.github.wooenrico.kafka.KafkaProperties;
import io.github.wooenrico.kafka.sender.DefaultKafkaProducer;
import io.github.wooenrico.kafka.sender.DefaultReactorKafkaSender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.testcontainers.containers.KafkaContainer;
import reactor.core.publisher.Flux;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = MainApp.class)
@Import(TestConfig.class)
public class KafkaIT {
    private static final Logger log = LoggerFactory.getLogger(KafkaIT.class);

    @Autowired
    private KafkaContainer kafkaContainer;

    @Autowired
    private KafkaProperties kafkaProperties;

    @Autowired
    @Qualifier("myHandler")
    private MyHandler myHandler;

    @Autowired
    @Qualifier("myReactorHandler")
    private MyReactorHandler myReactorHandler;

    @Autowired
    @Qualifier("defaultKafkaProducer")
    private DefaultKafkaProducer defaultKafkaProducer;

    @Autowired
    private DefaultReactorKafkaSender defaultReactorKafkaSender;

    @BeforeEach
    void setUp() {
        kafkaProperties.getCommonConsumerProperties()
                .setProperty("bootstrap.servers", kafkaContainer.getBootstrapServers());
        kafkaProperties.getCommonSenderProperties()
                .setProperty("bootstrap.servers", kafkaContainer.getBootstrapServers());
    }

    @Test
    void testMetricsKafkaConsumer() throws Exception {

        Flux.range(0, 100)
                .flatMap(i -> {
                    String s = "test" + i;
                    defaultKafkaProducer.send("test1", s);
                    return defaultReactorKafkaSender.send("test2", s);
                }).subscribe();

        assertTrue(myHandler.getLatch().await(30, TimeUnit.SECONDS), "Timeout waiting for sends");
        assertTrue(myReactorHandler.getLatch().await(30, TimeUnit.SECONDS), "Timeout waiting for receives");
    }
}
