import io.github.wooenrico.kafka.consumer.DefaultKafkaConsumer;
import io.github.wooenrico.kafka.consumer.DefaultKafkaReceiver;
import io.github.wooenrico.kafka.sender.DefaultKafkaProducer;
import io.github.wooenrico.kafka.sender.DefaultReactorKafkaSender;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;


import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class SenderConsumerIT extends AbstractKafkaContainerTest {
    private static final Logger log = LoggerFactory.getLogger(SenderConsumerIT.class);
    private static final String TOPIC = "test-topic";
    private static final int MESSAGE_COUNT = 100;

    @Test
    void testReactorSenderAndConsumer() throws Exception {
        CountDownLatch sendLatch = new CountDownLatch(MESSAGE_COUNT);
        CountDownLatch receiveLatch = new CountDownLatch(MESSAGE_COUNT);

        // Configure consumer
        consumerProperties.setTopic(Collections.singletonList(TOPIC));

        // Set up receiver
        try (DefaultKafkaReceiver receiver = new DefaultKafkaReceiver("test-receiver", consumerProperties,
                record -> {
                    log.info("Received: {}", record.value());
                    receiveLatch.countDown();
                    return reactor.core.publisher.Mono.empty();
                })) {

            // Set up sender
            try (DefaultReactorKafkaSender sender = new DefaultReactorKafkaSender(senderProperties,
                    result -> {
                        if (result.exception() != null) {
                            log.error("Send failed", result.exception());
                            fail("Send failed: " + result.exception().getMessage());
                        } else {
                            log.info("Sent message to {}", result.recordMetadata().topic());
                            sendLatch.countDown();
                        }
                    })) {

                // Send messages
                Flux.range(0, MESSAGE_COUNT)
                        .flatMap(i -> sender.send(TOPIC, String.valueOf(i)))
                        .subscribe();

                // Wait for all operations to complete
                assertTrue(sendLatch.await(30, TimeUnit.SECONDS), "Timeout waiting for sends");
                assertTrue(receiveLatch.await(30, TimeUnit.SECONDS), "Timeout waiting for receives");
            }
        }
    }

    @Test
    void testTraditionalSenderAndConsumer() throws Exception {
        CountDownLatch sendLatch = new CountDownLatch(MESSAGE_COUNT);
        CountDownLatch receiveLatch = new CountDownLatch(MESSAGE_COUNT);

        // Configure consumer
        consumerProperties.setTopic(Collections.singletonList(TOPIC));

        // Set up consumer
        try (DefaultKafkaConsumer consumer = new DefaultKafkaConsumer("test-consumer",
                consumerProperties,
                records -> {
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("Received: {}", record.value());
                        receiveLatch.countDown();
                    }
                })) {

            // Set up producer
            try (DefaultKafkaProducer producer = new DefaultKafkaProducer(senderProperties.getProperties())) {
                // Send messages
                for (int i = 0; i < MESSAGE_COUNT; i++) {
                    producer.send(TOPIC, String.valueOf(i), (metadata, exception) -> {
                        if (exception != null) {
                            log.error("Send failed", exception);
                            fail("Send failed: " + exception.getMessage());
                        } else {
                            log.info("Sent message to {}", metadata.topic());
                            sendLatch.countDown();
                        }
                    });
                }

                // Wait for all operations to complete
                assertTrue(sendLatch.await(30, TimeUnit.SECONDS), "Timeout waiting for sends");
                assertTrue(receiveLatch.await(30, TimeUnit.SECONDS), "Timeout waiting for receives");
            }
        }
    }
}
