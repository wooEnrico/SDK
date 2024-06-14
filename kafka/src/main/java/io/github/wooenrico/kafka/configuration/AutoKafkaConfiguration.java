package io.github.wooenrico.kafka.configuration;

import io.github.wooenrico.kafka.consumer.KafkaConsumerFactory;
import io.github.wooenrico.kafka.sender.DefaultReactorKafkaSender;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@ConditionalOnProperty(name = "kafka.configuration.enabled", matchIfMissing = false, havingValue = "true")
@EnableConfigurationProperties({KafkaProperties.class})
public class AutoKafkaConfiguration {

    @Bean
    @Primary
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
