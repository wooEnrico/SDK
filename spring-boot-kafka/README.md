# USEAGE

## version

![GitHub tag (latest by date)](https://img.shields.io/github/v/tag/wooEnrico/SDK)

## dependency

```xml

<dependencies>
    <dependency>
        <groupId>io.github.wooenrico</groupId>
        <artifactId>spring-boot-kafka</artifactId>
        <version>${tag#v}</version>
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

## consumer function implements

```properties
kafka.function.enabled=true
```

```java
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

@Configuration
public class TestConfiguration {

    @Bean("myHandler")
    public Consumer<ConsumerRecords<String, String>> myConsumer() {
        return records -> {
            //TODO
        };
    }

    @Bean("myReactorHandler")
    public Function<ConsumerRecord<String, String>, Mono<Void>> myReactorConsumer() {
        return record -> {
            //TODO
            return Mono.empty();
        };
    }
}
```

## consumer handler implements

### example1

[MyHandler.java](https://github.com/wooEnrico/SDK/blob/master/spring-boot-kafka/src/test/java/io/github/wooenrico/handler/MyHandler.java)

### example2

[MyReactorHandler.java](https://github.com/wooEnrico/SDK/blob/master/spring-boot-kafka/src/test/java/io/github/wooenrico/handler/MyReactorHandler.java)

## sender bean define

[TestConfig.java](https://github.com/wooEnrico/SDK/blob/master/spring-boot-kafka/src/test/java/io/github/wooenrico/config/TestConfig.java)

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
