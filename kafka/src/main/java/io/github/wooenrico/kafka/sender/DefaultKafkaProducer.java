package io.github.wooenrico.kafka.sender;

import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class DefaultKafkaProducer extends io.github.wooenrico.kafka.sender.KafkaProducer<String, String> {
    public DefaultKafkaProducer(Properties properties) {
        super(properties, new StringSerializer(), new StringSerializer());
    }
}
