package io.github.wooenrico.kafka.consumer;

import io.github.wooenrico.kafka.BeanNameRateLimitExecutorConsumerProperties;
import io.github.wooenrico.kafka.KafkaProperties;
import io.github.wooenrico.kafka.handler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class KafkaConsumerFactory extends AbstractKafkaConsumerFactory implements ApplicationContextAware, SmartInitializingSingleton {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerFactory.class);
    private ApplicationContext applicationContext;

    public KafkaConsumerFactory(KafkaProperties kafkaProperties) {
        super(kafkaProperties);
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

    protected void createConsumer(String key, BeanNameRateLimitExecutorConsumerProperties properties) throws Exception {

        if (key == null || key.isEmpty() || properties == null) {
            return;
        }

        String handlerBeanName = properties.getHandlerBeanName() != null && !properties.getHandlerBeanName().isEmpty() ? properties.getHandlerBeanName() : key;

        IKafkaHandler handler = this.applicationContext.getBean(handlerBeanName, IKafkaHandler.class);

        if (handler instanceof KafkaHandler) {
            this.createConsumer(key, properties, (DefaultKafkaHandler) handler);
        } else if (handler instanceof ReactorKafkaHandler) {
            this.createConsumer(key, properties, (DefaultReactorKafkaHandler) handler);
        } else {
            log.error("no kafka handler for {}", properties);
        }
    }
}
