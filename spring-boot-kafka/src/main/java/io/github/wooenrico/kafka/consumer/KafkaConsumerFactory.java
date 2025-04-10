package io.github.wooenrico.kafka.consumer;

import io.github.wooenrico.kafka.BeanNameRateLimitExecutorConsumerProperties;
import io.github.wooenrico.kafka.KafkaProperties;
import io.github.wooenrico.kafka.handler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;

public class KafkaConsumerFactory implements InitializingBean, DisposableBean, ApplicationContextAware, SmartInitializingSingleton {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerFactory.class);

    private final Set<Closeable> closeableObjects = new HashSet<>();
    private final KafkaProperties kafkaProperties;
    private ApplicationContext applicationContext;

    public KafkaConsumerFactory(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        for (String key : this.kafkaProperties.getConsumerSet()) {
            BeanNameRateLimitExecutorConsumerProperties consumerProperties = this.kafkaProperties.getConsumerProperties(key);
            this.createConsumer(key, consumerProperties);
        }
    }

    @Override
    public void afterSingletonsInstantiated() {
        log.info("kafka consumer factory initialized");
    }

    private void createConsumer(String key, BeanNameRateLimitExecutorConsumerProperties properties) throws Exception {

        if (key == null || key.isEmpty() || properties == null) {
            return;
        }

        String handlerBeanName = properties.getHandlerBeanName();

        IKafkaHandler handler = handlerBeanName != null ?
                this.applicationContext.getBean(handlerBeanName, IKafkaHandler.class)
                :
                this.applicationContext.getBean(key, IKafkaHandler.class);

        if (handler instanceof KafkaHandler) {
            DefaultKafkaHandler kafkaHandler = (DefaultKafkaHandler) handler;
            log.info("create kafka consumer {}, {}", properties, kafkaHandler);
            for (int i = 0; i < properties.getConcurrency(); i++) {
                DefaultKafkaConsumer kafkaConsumer = new DefaultKafkaConsumer(key + i, properties, kafkaHandler);
                this.closeableObjects.add(kafkaConsumer);
            }

        } else if (handler instanceof ReactorKafkaHandler) {
            DefaultReactorKafkaHandler reactorKafkaHandler = (DefaultReactorKafkaHandler) handler;
            log.info("create reactor kafka consumer {}, {}", properties, reactorKafkaHandler);
            for (int i = 0; i < properties.getConcurrency(); i++) {
                DefaultKafkaReceiver reactorKafkaReceiver = new DefaultKafkaReceiver(key + i, properties, reactorKafkaHandler);
                this.closeableObjects.add(reactorKafkaReceiver);
            }
        } else {
            log.error("no kafka handler for {}", properties);
        }
    }


    @Override
    public void destroy() throws Exception {
        for (Closeable closeable : this.closeableObjects) {
            closeable.close();
        }
    }
}
