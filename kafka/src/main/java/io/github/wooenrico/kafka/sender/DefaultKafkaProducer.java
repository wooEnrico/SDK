package io.github.wooenrico.kafka.sender;

import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.Properties;

public class DefaultKafkaProducer extends io.github.wooenrico.kafka.sender.KafkaProducer<String, String> implements InitializingBean, DisposableBean {
    private static final Logger log = LoggerFactory.getLogger(DefaultKafkaProducer.class);

    public DefaultKafkaProducer(Properties properties) {
        super(properties, new StringSerializer(), new StringSerializer());
    }

    @Override
    public void destroy() throws Exception {
        super.close();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("default kafka producer init with {}", this.properties);
    }
}
