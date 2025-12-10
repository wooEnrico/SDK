package io.github.wooenrico.handler;

import io.github.wooenrico.kafka.handler.DefaultSinksKafkaHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.concurrent.CountDownLatch;

@Service("mySinksHandler")
public class MySinksHandler implements DefaultSinksKafkaHandler {
    private static final Logger log = LoggerFactory.getLogger(MySinksHandler.class);
    private final CountDownLatch latch = new CountDownLatch(100);

    @Override
    public Mono<Void> apply(ConsumerRecord<String, String> stringStringConsumerRecord) {
        latch.countDown();
        log.info("Received message: {}", stringStringConsumerRecord.value());
        return Mono.empty();
    }

    public CountDownLatch getLatch() {
        return latch;
    }
}
