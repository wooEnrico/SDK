import io.github.wooenrico.kafka.KafkaProperties;
import io.github.wooenrico.kafka.consumer.*;
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

    private CountDownLatch getCountDownLatch() {
        return new CountDownLatch(100);
    }

    @Ignore
    @org.junit.Test
    public void testConsumer() throws Exception {

        CountDownLatch countDownLatch = getCountDownLatch();

        ConsumerProperties consumerProperties = KafkaProperties.LOCAL_CONSUMER;
        consumerProperties.setTopic(Collections.singletonList("test"));
        consumerProperties.setRate(10D);

        DefaultKafkaConsumer defaultKafkaConsumer = new DefaultKafkaConsumer("test1", consumerProperties, new Consumer<ConsumerRecord<String, String>>() {
            @Override
            public void accept(ConsumerRecord<String, String> stringStringConsumerRecord) {
                countDownLatch.countDown();
                log.info("{}", stringStringConsumerRecord.value());
            }
        });

        countDownLatch.await();

        defaultKafkaConsumer.close();
    }

    @Ignore
    @org.junit.Test
    public void testReactorConsumer() throws Exception {

        CountDownLatch countDownLatch = getCountDownLatch();

        ConsumerProperties consumerProperties = KafkaProperties.LOCAL_CONSUMER;
        consumerProperties.setTopic(Collections.singletonList("test"));
        consumerProperties.setRate(1D);

        DefaultReactorKafkaReceiver defaultReactorKafkaReceiver = new DefaultReactorKafkaReceiver("reactor-test1", consumerProperties, new Function<ConsumerRecord<String, String>, Mono<Void>>() {
            @Override
            public Mono<Void> apply(ConsumerRecord<String, String> stringStringConsumerRecord) {
                countDownLatch.countDown();
                log.info("{}", stringStringConsumerRecord.value());
                return Mono.empty();
            }
        });

        countDownLatch.await();

        defaultReactorKafkaReceiver.close();
    }
}
