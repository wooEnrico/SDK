package io.github.wooenrico.kafka.annotation;

import io.github.wooenrico.kafka.configuration.AutoKafkaConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(value = {AutoKafkaConfiguration.class})
public @interface AutoKafka {
}
