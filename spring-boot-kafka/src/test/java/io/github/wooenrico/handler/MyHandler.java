package io.github.wooenrico.handler;

import io.github.wooenrico.kafka.handler.DefaultKafkaHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

@Service("myHandler")
public class MyHandler implements DefaultKafkaHandler {
    private static final Logger log = LoggerFactory.getLogger(MyHandler.class);
    private final CountDownLatch latch = new CountDownLatch(100);

    @Override
    public void accept(ConsumerRecord<String, String> record) {
        latch.countDown();
        log.info("Received message: {}", record.value());
    }

    public CountDownLatch getLatch() {
        return latch;
    }
}
