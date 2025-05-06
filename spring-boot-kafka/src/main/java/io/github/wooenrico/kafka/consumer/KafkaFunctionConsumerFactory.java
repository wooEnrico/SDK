package io.github.wooenrico.kafka.consumer;

import io.github.wooenrico.kafka.BeanNameRateLimitExecutorConsumerProperties;
import io.github.wooenrico.kafka.KafkaProperties;
import io.github.wooenrico.kafka.util.FunctionUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class KafkaFunctionConsumerFactory extends AbstractKafkaConsumerFactory implements SmartInitializingSingleton {
    private static final Logger log = LoggerFactory.getLogger(KafkaFunctionConsumerFactory.class);
    private final FunctionCatalog functionCatalog;

    public KafkaFunctionConsumerFactory(KafkaProperties kafkaProperties, FunctionCatalog functionCatalog) {
        super(kafkaProperties);
        this.functionCatalog = functionCatalog;
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
        log.info("kafka function consumer factory initialized");
    }

    protected void createConsumer(String key, BeanNameRateLimitExecutorConsumerProperties properties) throws Exception {

        if (key == null || key.isEmpty() || properties == null) {
            return;
        }

        String handlerBeanName = properties.getHandlerBeanName() != null && !properties.getHandlerBeanName().isEmpty() ? properties.getHandlerBeanName() : key;

        SimpleFunctionRegistry.FunctionInvocationWrapper lookup = this.functionCatalog.lookup(handlerBeanName);

        if (lookup == null) {
            log.error("no kafka handler for {}", properties);
            return;
        }

        this.resetProperties(this.functionCatalog, properties);

        if (FunctionUtil.isConsumerOf(lookup, ConsumerRecord.class)) {
            Consumer<ConsumerRecord<String, String>> consumer = this.functionCatalog.lookup(handlerBeanName);
            this.createConsumer(key, properties, consumer);
        } else if (FunctionUtil.isFunctionOf(lookup, ConsumerRecord.class, Mono.class)) {
            Function<ConsumerRecord<String, String>, Mono<Void>> function = this.functionCatalog.lookup(handlerBeanName);
            this.createConsumer(key, properties, function);
        } else {
            log.error("no kafka handler for {}", properties);
        }
    }

    private void resetProperties(FunctionCatalog functionCatalog, BeanNameRateLimitExecutorConsumerProperties properties) {
        if (properties.getTopicFunctionName() != null && !properties.getTopicFunctionName().isEmpty()) {
            SimpleFunctionRegistry.FunctionInvocationWrapper lookup = functionCatalog.lookup(properties.getTopicFunctionName());
            if (FunctionUtil.isFunctionOf(lookup, List.class, List.class)) {
                Function<List<String>, List<String>> function = functionCatalog.lookup(properties.getTopicFunctionName());
                properties.setTopic(function.apply(properties.getTopic()));
            }
        }

        if (properties.getConcurrencyFunctionName() != null && !properties.getConcurrencyFunctionName().isEmpty()) {
            SimpleFunctionRegistry.FunctionInvocationWrapper lookup = functionCatalog.lookup(properties.getConcurrencyFunctionName());
            if (FunctionUtil.isFunctionOf(lookup, Integer.class, Integer.class)) {
                Function<Integer, Integer> function = functionCatalog.lookup(properties.getConcurrencyFunctionName());
                properties.setConcurrency(function.apply(properties.getConcurrency()));
            }
        }
    }
}
