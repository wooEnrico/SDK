# USEAGE

## dependency

```xml

<dependencies>
    <dependency>
        <groupId>io.github.wooenrico</groupId>
        <artifactId>spring-boot-kafka</artifactId>
        <version>1.0.8</version>
    </dependency>
</dependencies>
```

if dependency omitted for duplicate, you can use below dependency instead.

```xml

<dependencies>
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>3.0.0</version>
    </dependency>
    <dependency>
        <groupId>io.projectreactor.kafka</groupId>
        <artifactId>reactor-kafka</artifactId>
        <version>1.3.19</version>
    </dependency>
    <dependency>
        <groupId>io.projectreactor</groupId>
        <artifactId>reactor-core</artifactId>
        <version>3.4.25</version>
    </dependency>
</dependencies>
```

## consumer handler implements

### example1

```java

import org.apache.kafka.clients.consumer.ConsumerRecord;
import io.github.wooenrico.kafka.handler.DefaultKafkaHandler;

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
import io.github.wooenrico.kafka.handler.DefaultReactorKafkaHandler;

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
import io.github.wooenrico.kafka.KafkaProperties;
import io.github.wooenrico.kafka.annotation.AutoKafka;
import io.github.wooenrico.kafka.sender.DefaultReactorKafkaSender;
import io.github.wooenrico.kafka.sender.DefaultKafkaProducer;

@Configuration
@AutoKafka
public class MyConfiguration {
    
    @Bean("testReactorKafkaSender")
    @ConditionalOnProperty(name = "kafka.sender.test.enabled", matchIfMissing = false, havingValue = "true")
    public DefaultReactorKafkaSender reactorKafkaSender(KafkaProperties kafkaProperties) {
        return new DefaultReactorKafkaSender(kafkaProperties.getSenderProperties("test"));
    }

    @Bean("test2KafkaProducer")
    @ConditionalOnProperty(name = "kafka.sender.test2.enabled", matchIfMissing = false, havingValue = "true")
    public DefaultKafkaProducer reactorKafkaSender(KafkaProperties kafkaProperties) {
        return new DefaultKafkaProducer(kafkaProperties.getSenderProperties("test2").getProperties());
    }
}
```

### autowired

```java

import io.github.wooenrico.kafka.sender.DefaultReactorKafkaSender;
import io.github.wooenrico.kafka.sender.DefaultKafkaProducer;

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