![GitHub tag (latest by date)](https://img.shields.io/github/v/tag/wooEnrico/SDK)
![GitHub issues](https://img.shields.io/github/issues/wooEnrico/SDK)
![GitHub repo size](https://img.shields.io/github/repo-size/wooEnrico/SDK)
![GitHub Workflow Status (with branch)](https://img.shields.io/github/actions/workflow/status/wooEnrico/SDK/maven.yml?branch=master)

# SDK

[Java Software Development Kit](https://wooenrico.github.io/SDK/)

# USEAGE

* [kafka](https://github.com/wooEnrico/SDK/blob/master/kafka/README.md)
* [spring-boot-kafka-starter](https://github.com/wooEnrico/SDK/blob/master/spring-boot-starter/spring-boot-kafka-starter/README.md)
* [spring-boot3-kafka-starter](https://github.com/wooEnrico/SDK/blob/master/spring-boot-starter/spring-boot3-kafka-starter/README.md)
* [spring-boot-data-redis](https://github.com/wooEnrico/SDK/blob/redis-2.7.5/README.md)
* [spring-boot3-data-redis](https://github.com/wooEnrico/SDK/tree/redis-3.1.2/README.md)

# spring-boot-kafka

```xml

<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-kafka</artifactId>
    </dependency>
</dependencies>
```

## properties

```properties
spring.kafka.source.bootstrap.servers=localhost:9092
spring.kafka.source.group.id=test
spring.kafka.source.auto.offset.reset=latest
spring.kafka.source.enable.auto.commit=true
spring.kafka.target.bootstrap.servers=localhost:29092
spring.kafka.target.acks=-1
spring.kafka.target.retries=3
spring.kafka.target.batch.size=16384
```

## configuration

```java
@Configuration
public class KafkaConfig {
    // ================== Source Kafka (Consumer) ==================

    @Bean
    @ConfigurationProperties(prefix = "spring.kafka.source")
    public Map<String, String> sourceKafkaProperties() {
        return new HashMap<>();
    }

    @Bean
    public ConsumerFactory<String, String> sourceConsumerFactory() {
        Map<String, Object> props = new HashMap<>(sourceKafkaProperties());
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean(name = "sourceContainerFactory")
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> sourceContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(sourceConsumerFactory());
        factory.setBatchListener(true);
        return factory;
    }

    // ================== Target Kafka (Producer) ==================

    @Bean
    @ConfigurationProperties(prefix = "spring.kafka.target")
    public Map<String, String> targetKafkaProperties() {
        return new HashMap<>();
    }

    @Bean
    public ProducerFactory<String, String> targetProducerFactory() {
        Map<String, Object> props = new HashMap<>(targetKafkaProperties());
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean(name = "targetKafkaTemplate")
    public KafkaTemplate<String, String> targetKafkaTemplate() {
        return new KafkaTemplate<>(targetProducerFactory());
    }
}
```

## example

```java
@Slf4j
@Component
public class MyKafkaListener {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public MyKafkaListener(@Qualifier("targetKafkaTemplate") KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "test", containerFactory = "sourceContainerFactory")
    public void processMessage(List<ConsumerRecord<String, String>> consumerRecords) {
        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            log.info("Received message: {} from topic: {}, partition: {}", consumerRecord.value(), consumerRecord.topic(), consumerRecord.partition());
            // Add your message processing logic here
            kafkaTemplate.send("test", consumerRecord.value())
                    .thenAccept(result -> {
                        log.info("Message forwarded to target Kafka topic 'test': {}", consumerRecord.value());
                    });
        }
    }
}
```

# spring-data-jdbc

## dependency

```xml

<dependencies>
    <dependency>
        <groupId>mysql</groupId>
        <artifactId>mysql-connector-java</artifactId>
        <version>8.0.28</version>
    </dependency>
    <dependency>
        <groupId>com.zaxxer</groupId>
        <artifactId>HikariCP</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.data</groupId>
        <artifactId>spring-data-jdbc</artifactId>
    </dependency>
</dependencies>
```

## properties

```properties
test.jdbc.driver-class-name=com.mysql.cj.jdbc.Driver
test.jdbc.jdbc-url=jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false
test.jdbc.username=root
test.jdbc.password=123456
test.jdbc.connection-timeout=10000
test.jdbc.idle-timeout=60000
test.jdbc.max-lifetime=1800000
test.jdbc.maximum-pool-size=50
test.jdbc.minimum-idle=5
```

## example

```java

@Bean
@ConfigurationProperties(prefix = "test.jdbc")
public HikariConfig testHikariConfig() {
    return new HikariConfig();
}

@Bean
public JdbcTemplate testJdbcTemplate(HikariConfig testHikariConfig) {
    HikariDataSource hikariDataSource = new HikariDataSource(testHikariConfig);
    return new JdbcTemplate(hikariDataSource);
}
```
