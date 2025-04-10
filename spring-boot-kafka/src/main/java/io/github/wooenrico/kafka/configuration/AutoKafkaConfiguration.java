package io.github.wooenrico.kafka.configuration;

import io.github.wooenrico.kafka.KafkaProperties;
import io.github.wooenrico.kafka.consumer.KafkaConsumerFactory;
import io.github.wooenrico.kafka.sender.DefaultReactorKafkaSender;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@ConditionalOnProperty(name = "kafka.configuration.enabled", matchIfMissing = false, havingValue = "true")
public class AutoKafkaConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "kafka")
    public KafkaProperties kafkaProperties() {
        return new KafkaProperties();
    }

    @Bean
    @Primary
    @ConditionalOnMissingBean(KafkaConsumerFactory.class)
    public KafkaConsumerFactory getKafkaConsumerFactory(KafkaProperties kafkaProperties) {
        return new KafkaConsumerFactory(kafkaProperties);
    }

    @Bean
    @Primary
    @ConditionalOnProperty(name = "kafka.sender.primary.enabled", matchIfMissing = false, havingValue = "true")
    public DefaultReactorKafkaSender reactorKafkaSender(KafkaProperties kafkaProperties) {
        return new DefaultReactorKafkaSender(kafkaProperties.getSenderProperties("primary"));
    }
}
