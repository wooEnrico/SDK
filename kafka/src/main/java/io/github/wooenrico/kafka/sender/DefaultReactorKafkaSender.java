package io.github.wooenrico.kafka.sender;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import reactor.kafka.sender.SenderResult;

import java.util.function.Consumer;

public class DefaultReactorKafkaSender extends ProducerRecordReactorKafkaSender<String, String> implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(DefaultReactorKafkaSender.class);

    public DefaultReactorKafkaSender(SenderProperties properties) {
        super(properties, new StringSerializer(), new StringSerializer());
    }

    public DefaultReactorKafkaSender(SenderProperties properties, Consumer<SenderResult<ProducerRecord<String, String>>> senderResultConsumer) {
        super(properties, new StringSerializer(), new StringSerializer(), senderResultConsumer);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("default reactor kafka sender init with {}", this.properties);
    }

    @Override
    public void destroy() throws Exception {
        super.dispose();
    }
}
