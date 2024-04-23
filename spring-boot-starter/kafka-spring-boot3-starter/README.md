# USEAGE

## dependency

you can use in spring boot 3.x

```xml
<dependencies>
    <dependency>
        <groupId>io.github.wooenrico</groupId>
        <artifactId>kafka-spring-boot3-starter</artifactId>
        <version>1.0.1</version>
    </dependency>
</dependencies>
```

## consumer handler implements

### example1

```java

import org.apache.kafka.clients.consumer.ConsumerRecord;
import io.github.wooernico.kafka.handler.DefaultKafkaHandler;

@Service("myHandler")
public class MyHandler implements DefaultKafkaHandler {
    @Override
    public void accept(ConsumerRecord<String, String> record) {
        //TODO
    }
}
```

### example2

```java
import reactor.core.publisher.Mono;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import io.github.wooernico.kafka.handler.DefaultReactorKafkaHandler;

@Service("myReactorHandler")
public class MyReactorHandler implements DefaultReactorKafkaHandler {
    @Override
    public Mono<Void> apply(ConsumerRecord<String, String> record) {
        //TODO
        return Mono.empty();
    }
}
```

## sender bean define & autowired

### define

```java
import io.github.wooernico.kafka.configuration.KafkaProperties;
import io.github.wooernico.kafka.sender.DefaultReactorKafkaSender;
import io.github.wooernico.kafka.sender.DefaultKafkaProducer;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class MyConfiguration {
    @Bean("testReactorKafkaSender")
    @ConditionalOnProperty(name = "kafka.sender.test.enabled", matchIfMissing = false, havingValue = "true")
    public DefaultReactorKafkaSender reactorKafkaSender(KafkaProperties kafkaProperties) {
        return new DefaultReactorKafkaSender(kafkaProperties.getSender().get("test"));
    }

    @Bean("test2KafkaProducer")
    @ConditionalOnProperty(name = "kafka.sender.test2.enabled", matchIfMissing = false, havingValue = "true")
    public DefaultKafkaProducer reactorKafkaSender(KafkaProperties kafkaProperties) {
        return new DefaultKafkaProducer(kafkaProperties.getSender().get("test2").getProperties());
    }
}
```

### autowired

```java

import io.github.wooernico.kafka.sender.DefaultReactorKafkaSender;
import io.github.wooernico.kafka.sender.DefaultKafkaProducer;

@Service
public class SenderTest {
    @Autowired
    private DefaultReactorKafkaSender reactorKafkaSender;
    @Autowired
    @Qualifier("testReactorKafkaSender")
    private DefaultReactorKafkaSender testReactorKafkaSender;
    @Autowired
    @Qualifier("test2KafkaProducer")
    private DefaultKafkaProducer kafkaProducer;
}
```

## properties

```properties
########################################################################################################################
## TODO START auto kafka configuration
########################################################################################################################
## kafka 自动配置选项
kafka.configuration.enabled=true
## TODO kafka consumer conf
## kafka consumer 通用配置选项
kafka.common-consumer-properties.bootstrap.servers=127.0.0.1:9092
### example-1 简单配置，myHandler是消费者名称又是处理器名称
kafka.consumer.myHandler.enabled=true
kafka.consumer.myHandler.properties.group.id=group1
kafka.consumer.myHandler.topic=test1
kafka.consumer.myHandler.concurrency=1
### example-2 自定义消费者名称customName, 并配置处理器名称myReactorHandler
kafka.consumer.customName.enabled=true
kafka.consumer.customName.handlerBeanName=myReactorHandler
kafka.consumer.customName.properties.bootstrap.servers=127.0.0.1:9093
kafka.consumer.customName.properties.group.id=group2
kafka.consumer.customName.topic=test2
kafka.consumer.customName.concurrency=1
## TODO kafka sender conf
## kafka sender 通用配置选项
kafka.common-sender-properties.bootstrap.servers=127.0.0.1:9092
## kafka sender 默认spring bean配置选项
kafka.sender.primary.enabled=true
## kafka sender 自定义spring bean配置选项
kafka.sender.test.enabled=true
kafka.sender.test.properties.bootstrap.servers=127.0.0.1:9093
########################################################################################################################
## TODO END auto kafka configuration
########################################################################################################################
```