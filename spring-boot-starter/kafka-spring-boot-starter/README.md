# USEAGE

## dependency

you can use in spring boot 2.x

```xml

<dependencies>

    <dependency>
        <groupId>io.github.wooenrico</groupId>
        <artifactId>kafka-spring-boot-starter</artifactId>
        <version>1.0.0</version>
    </dependency>

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

@Service("myHandler")
public class MyHandler implements io.github.wooernico.kafka.handler.KafkaHandler {
    @Override
    public void accept(ConsumerRecord<String, String> record) {
        //TODO
    }
}
```

### example2

```java

@Service("myReactorHandler")
public class MyReactorHandler implements io.github.wooernico.kafka.handler.ReactorKafkaHandler {
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

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class MyConfiguration {
    @Bean("testReactorKafkaSender")
    @ConditionalOnProperty(name = "kafka.sender.test.enabled", matchIfMissing = false, havingValue = "true")
    public ReactorKafkaSender<String, String, Object> reactorKafkaSender(KafkaProperties kafkaProperties) {
        return new io.github.wooernico.kafka.sender.ReactorKafkaSender<>(kafkaProperties.getSender().get("test"));
    }

    @Bean("test2KafkaProducer")
    @ConditionalOnProperty(name = "kafka.sender.test2.enabled", matchIfMissing = false, havingValue = "true")
    public KafkaProducer<String, String> reactorKafkaSender(KafkaProperties kafkaProperties) {
        return new io.github.wooernico.kafka.sender.KafkaProducer<>(kafkaProperties.getSender().get("test2").getProperties());
    }
}
```

### autowired

```java

@Service
public class Sender implements InitializingBean {
    @Autowired
    private ReactorKafkaSender<String, String, Object> reactorKafkaSender;
    @Autowired
    @Qualifier("testReactorKafkaSender")
    private ReactorKafkaSender<String, String, Object> testReactorKafkaSender;

    @Autowired
    @Qualifier("test2KafkaProducer")
    private io.github.wooernico.kafka.sender.KafkaProducer<String, String> kafkaProducer;


    @Override
    public void afterPropertiesSet() throws Exception {

    }
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