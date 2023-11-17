package io.github.wooernico.kafka.consumer;

import io.github.wooernico.kafka.configuration.KafkaProperties;
import io.github.wooernico.kafka.handler.IKafkaHandler;
import io.github.wooernico.kafka.handler.KafkaHandler;
import io.github.wooernico.kafka.handler.ReactorKafkaHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KafkaConsumerFactory implements InitializingBean, DisposableBean, ApplicationContextAware {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerFactory.class);

    private final Set<ReactorKafkaReceiver> reactorKafkaReceivers = new HashSet<>();
    private final Set<KafkaConsumer> kafkaConsumers = new HashSet<>();
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
        Map<String, ConsumerProperties> consumer = this.kafkaProperties.getConsumer();

        for (Map.Entry<String, ConsumerProperties> entry : consumer.entrySet()) {
            String key = entry.getKey();
            ConsumerProperties properties = entry.getValue();

            if (!properties.isEnabled()) {
                continue;
            }

            String handlerBeanName = properties.getHandlerBeanName();

            IKafkaHandler handler = handlerBeanName != null ?
                    this.applicationContext.getBean(handlerBeanName, IKafkaHandler.class)
                    :
                    this.applicationContext.getBean(key, IKafkaHandler.class);

            if (handler instanceof KafkaHandler) {
                KafkaHandler kafkaHandler = (KafkaHandler) handler;
                for (int i = 0; i < properties.getConcurrency(); i++) {
                    KafkaConsumer kafkaConsumer = new KafkaConsumer(key + i, properties, kafkaHandler);
                    kafkaConsumer.afterPropertiesSet();
                    this.kafkaConsumers.add(kafkaConsumer);
                }

            } else if (handler instanceof ReactorKafkaHandler) {
                ReactorKafkaHandler reactorKafkaHandler = (ReactorKafkaHandler) handler;
                for (int i = 0; i < properties.getConcurrency(); i++) {
                    ReactorKafkaReceiver reactorKafkaReceiver = new ReactorKafkaReceiver(key + i, properties, reactorKafkaHandler);
                    reactorKafkaReceiver.afterPropertiesSet();
                    this.reactorKafkaReceivers.add(reactorKafkaReceiver);
                }
            } else {
                log.error("no kafka handler for {}", properties);
            }
        }
    }


    @Override
    public void destroy() throws Exception {
        this.reactorKafkaReceivers.forEach(ReactorKafkaReceiver::dispose);
        this.kafkaConsumers.forEach(KafkaConsumer::dispose);
    }
}
