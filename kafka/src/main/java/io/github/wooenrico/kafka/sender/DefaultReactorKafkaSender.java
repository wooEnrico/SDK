package io.github.wooenrico.kafka.sender;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.kafka.sender.SenderResult;

import java.util.function.Consumer;

public class DefaultReactorKafkaSender extends ProducerRecordReactorKafkaSender<String, String> {

    public DefaultReactorKafkaSender(SenderProperties properties) {
        super(properties, new StringSerializer(), new StringSerializer());
    }

    public DefaultReactorKafkaSender(SenderProperties properties, Consumer<SenderResult<ProducerRecord<String, String>>> senderResultConsumer) {
        super(properties, new StringSerializer(), new StringSerializer(), senderResultConsumer);
    }
}
