package io.github.wooernico.kafka.configuration;

import io.github.wooernico.kafka.consumer.KafkaConsumerFactory;
import io.github.wooernico.kafka.sender.ReactorKafkaSender;
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
    public ReactorKafkaSender<String, String, Object> reactorKafkaSender(KafkaProperties kafkaProperties) {
        return new ReactorKafkaSender<String, String, Object>(kafkaProperties.getSender().get("primary"));
    }
}
