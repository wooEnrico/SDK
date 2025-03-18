import io.github.wooenrico.kafka.consumer.ConsumerProperties;
import io.github.wooenrico.kafka.consumer.DefaultKafkaConsumer;
import io.github.wooenrico.kafka.consumer.DefaultKafkaReceiver;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;

public class TestConsumer {
    private static final Logger log = LoggerFactory.getLogger(TestConsumer.class);

    public static ConsumerProperties LOCAL_CONSUMER = new ConsumerProperties() {
        {
            setEnabled(true);
            addProperties(ConsumerConfig.GROUP_ID_CONFIG, "local-consumer");
            addProperties(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            addProperties(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            addProperties(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        }
    };

    @Ignore
    @org.junit.Test
    public void testConsumer() throws Exception {

        CountDownLatch countDownLatch = new CountDownLatch(100);

        // kafka consumer properties
        ConsumerProperties consumerProperties = LOCAL_CONSUMER;
        consumerProperties.setTopic(Collections.singletonList("test"));

        // record handler
        Consumer<ConsumerRecord<String, String>> handler = stringStringConsumerRecord -> {
            countDownLatch.countDown();
            log.info("{}", stringStringConsumerRecord.value());
        };

        // consumer
        try (DefaultKafkaConsumer defaultKafkaConsumer = new DefaultKafkaConsumer("test1", consumerProperties, handler)) {
            countDownLatch.await();
        } catch (Exception e) {
            log.error("consumer kafka record error", e);
        }
    }

    @Ignore
    @org.junit.Test
    public void testReactorConsumer() throws Exception {

        CountDownLatch countDownLatch = new CountDownLatch(100);

        // kafka consumer properties
        ConsumerProperties consumerProperties = LOCAL_CONSUMER;
        consumerProperties.setTopic(Collections.singletonList("test"));
        // record handler
        Function<ConsumerRecord<String, String>, Mono<Void>> handler = stringStringConsumerRecord -> {
            countDownLatch.countDown();
            log.info("{}", stringStringConsumerRecord.value());
            return Mono.empty();
        };

        // reactor consumer
        try (DefaultKafkaReceiver defaultReactorKafkaReceiver = new DefaultKafkaReceiver("reactor-test1", consumerProperties, handler)) {
            countDownLatch.await();
        } catch (Exception e) {
            log.error("reactor consumer kafka record error", e);
        }
    }
}
