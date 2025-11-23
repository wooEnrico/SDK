import io.github.wooenrico.kafka.consumer.DefaultKafkaConsumer;
import io.github.wooenrico.kafka.consumer.DefaultKafkaReceiver;
import io.github.wooenrico.kafka.consumer.DefaultSinksKafkaConsumer;
import io.github.wooenrico.kafka.consumer.RateLimitExecutorConsumerProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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

    public static RateLimitExecutorConsumerProperties LOCAL_CONSUMER = new RateLimitExecutorConsumerProperties() {
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
        RateLimitExecutorConsumerProperties consumerProperties = LOCAL_CONSUMER;
        consumerProperties.setTopic(Collections.singletonList("test"));

        // record handler
        Consumer<ConsumerRecords<String, String>> handler = records -> {
            for (ConsumerRecord<String, String> record : records) {
                countDownLatch.countDown();
                log.info("{}", record.value());
            }
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
        RateLimitExecutorConsumerProperties consumerProperties = LOCAL_CONSUMER;
        consumerProperties.setTopic(Collections.singletonList("test"));
        // record handler
        Function<ConsumerRecord<String, String>, Mono<Void>> handler = stringStringConsumerRecord -> {
            countDownLatch.countDown();
            log.info("{}", stringStringConsumerRecord.value());
            return Mono.empty();
        };

        // reactor consumer
        try (DefaultSinksKafkaConsumer defaultReactorKafkaReceiver = new DefaultSinksKafkaConsumer("sinks-test1", consumerProperties, handler)) {
            countDownLatch.await();
        } catch (Exception e) {
            log.error("reactor consumer kafka record error", e);
        }
    }

    @Ignore
    @org.junit.Test
    public void testReactorReceiver() throws Exception {

        CountDownLatch countDownLatch = new CountDownLatch(1000000);

        // kafka consumer properties
        RateLimitExecutorConsumerProperties consumerProperties = LOCAL_CONSUMER;
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
            log.error("reactor receiver kafka record error", e);
        }
    }
}
