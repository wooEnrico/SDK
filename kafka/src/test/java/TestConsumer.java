import io.github.wooenrico.kafka.consumer.*;
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

    @Ignore
    @org.junit.Test
    public void testConsumer() throws Exception {

        ConsumerProperties consumerProperties = getConsumerProperties();

        DefaultKafkaConsumer test = new DefaultKafkaConsumer("test1", consumerProperties, new Consumer<ConsumerRecord<String, String>>() {
            @Override
            public void accept(ConsumerRecord<String, String> stringStringConsumerRecord) {
                log.info("{}", stringStringConsumerRecord.value());
            }
        });
        test.afterPropertiesSet();

        new CountDownLatch(1).await();
    }

    @Ignore
    @org.junit.Test
    public void testReactorConsumer() throws Exception {

        ConsumerProperties consumerProperties = getConsumerProperties();

        DefaultReactorKafkaReceiver test2 = new DefaultReactorKafkaReceiver("reactor-test1", consumerProperties, new Function<ConsumerRecord<String, String>, Mono<Void>>() {
            @Override
            public Mono<Void> apply(ConsumerRecord<String, String> stringStringConsumerRecord) {
                log.info("{}", stringStringConsumerRecord.value());
                return Mono.empty();
            }
        });

        test2.afterPropertiesSet();

        new CountDownLatch(1).await();
    }

    private static ConsumerProperties getConsumerProperties() {
        ConsumerProperties consumerProperties = new ConsumerProperties();
        consumerProperties.addProperties(ConsumerConfig.GROUP_ID_CONFIG, "test-group-id");
        consumerProperties.setTopic(Collections.singletonList("test"));
        return consumerProperties;
    }
}
