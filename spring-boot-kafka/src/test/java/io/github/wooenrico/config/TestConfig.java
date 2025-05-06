package io.github.wooenrico.config;

import io.github.wooenrico.handler.MyHandler;
import io.github.wooenrico.handler.MyReactorHandler;
import io.github.wooenrico.kafka.KafkaProperties;
import io.github.wooenrico.kafka.annotation.AutoKafka;
import io.github.wooenrico.kafka.sender.DefaultKafkaProducer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@AutoKafka
@TestConfiguration
public class TestConfig {

    @Bean(initMethod = "start", destroyMethod = "stop")
    public KafkaContainer kafkaContainer() {
        return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
    }

    @Bean("myReactorHandler")
    public MyReactorHandler myReactorHandler() {
        return new MyReactorHandler();
    }

    @Bean("myHandler")
    public MyHandler myHandler() {
        return new MyHandler();
    }

    @Bean("defaultKafkaProducer")
    @ConditionalOnProperty(name = "kafka.sender.test.enabled", matchIfMissing = false, havingValue = "true")
    public DefaultKafkaProducer reactorKafkaSender(KafkaProperties kafkaProperties) {
        return new DefaultKafkaProducer(kafkaProperties.getSenderProperties("test").getProperties());
    }

    @Bean
    public Function<List<String>, List<String>> myTopicFunction() {
        return list -> list.stream().filter(s -> s.equals("test1")).collect(Collectors.toList());
    }

    @Bean
    public Function<List<String>, List<String>> myTopicFunction2() {
        return list -> list.stream().filter(s -> s.equals("test2")).collect(Collectors.toList());
    }
} 
