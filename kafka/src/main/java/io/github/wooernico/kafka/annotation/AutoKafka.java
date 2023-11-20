package io.github.wooernico.kafka.annotation;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(value = {io.github.wooernico.kafka.configuration.AutoKafkaConfiguration.class})
public @interface AutoKafka {
}
