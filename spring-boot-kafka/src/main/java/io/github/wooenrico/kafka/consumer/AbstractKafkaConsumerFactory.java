package io.github.wooenrico.kafka.consumer;

import io.github.wooenrico.kafka.BeanNameRateLimitExecutorConsumerProperties;
import io.github.wooenrico.kafka.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import reactor.core.publisher.Mono;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class AbstractKafkaConsumerFactory implements InitializingBean, DisposableBean {
    private static final Logger log = LoggerFactory.getLogger(AbstractKafkaConsumerFactory.class);

    private final Set<Closeable> closeableObjects = new HashSet<>();
    protected final KafkaProperties kafkaProperties;

    public AbstractKafkaConsumerFactory(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        for (String key : this.kafkaProperties.getConsumerSet()) {
            BeanNameRateLimitExecutorConsumerProperties consumerProperties = this.kafkaProperties.getConsumerProperties(key);
            this.createConsumer(key, consumerProperties);
        }
    }

    protected abstract void createConsumer(String key, BeanNameRateLimitExecutorConsumerProperties properties) throws Exception;

    protected void createConsumer(String key, BeanNameRateLimitExecutorConsumerProperties properties, Consumer<ConsumerRecords<String, String>> consumer) throws Exception {
        log.info("create kafka consumer {}, {}", properties, consumer);
        for (int i = 0; i < properties.getConcurrency(); i++) {
            DefaultKafkaConsumer kafkaConsumer = new DefaultKafkaConsumer(key + i, properties, consumer);
            this.closeableObjects.add(kafkaConsumer);
        }
    }

    protected void createConsumer(String key, BeanNameRateLimitExecutorConsumerProperties properties, Function<ConsumerRecord<String, String>, Mono<Void>> function) throws Exception {
        log.info("create reactor kafka consumer {}, {}", properties, function);
        for (int i = 0; i < properties.getConcurrency(); i++) {
            DefaultKafkaReceiver reactorKafkaReceiver = new DefaultKafkaReceiver(key + i, properties, function);
            this.closeableObjects.add(reactorKafkaReceiver);
        }
    }

    @Override
    public void destroy() throws Exception {
        for (Closeable closeable : this.closeableObjects) {
            closeable.close();
        }
    }
}
