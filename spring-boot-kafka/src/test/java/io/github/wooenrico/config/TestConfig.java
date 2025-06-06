package io.github.wooenrico.config;

import io.github.wooenrico.kafka.KafkaProperties;
import io.github.wooenrico.kafka.annotation.AutoKafka;
import io.github.wooenrico.kafka.sender.DefaultKafkaProducer;
import io.github.wooenrico.kafka.sender.DefaultReactorKafkaSender;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@AutoKafka
@TestConfiguration
public class TestConfig {

    @Bean(initMethod = "start", destroyMethod = "stop")
    public KafkaContainer kafkaContainer() {
        return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
    }

    @Bean("defaultKafkaProducer")
    @ConditionalOnProperty(name = "kafka.sender.test.enabled", matchIfMissing = false, havingValue = "true")
    public DefaultKafkaProducer kafkaSender(KafkaProperties kafkaProperties) {
        return new DefaultKafkaProducer(kafkaProperties.getSenderProperties("test").getProperties());
    }

    @Bean("defaultReactorKafkaSender")
    @ConditionalOnProperty(name = "kafka.sender.test.enabled", matchIfMissing = false, havingValue = "true")
    public DefaultReactorKafkaSender reactorKafkaSender(KafkaProperties kafkaProperties) {
        return new DefaultReactorKafkaSender(kafkaProperties.getSenderProperties("test"));
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
