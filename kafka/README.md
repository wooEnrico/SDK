# USEAGE

## dependency

```xml

<dependencies>
    <dependency>
        <groupId>io.github.wooenrico</groupId>
        <artifactId>kafka</artifactId>
        <version>1.0.7</version>
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

## consumer

### example1

```java

@org.junit.Test
public void testConsumer() throws Exception {


    CountDownLatch countDownLatch = new CountDownLatch(100);

    // kafka consumer properties
    ConsumerProperties consumerProperties = KafkaProperties.LOCAL_CONSUMER;
    consumerProperties.setTopic(Collections.singletonList("test"));

    // record handler
    Consumer<ConsumerRecord<String, String>> handler = stringStringConsumerRecord -> {
        countDownLatch.countDown();
        log.info("{}", stringStringConsumerRecord.value());
    };

    // consumer
    try (DefaultKafkaConsumer defaultKafkaConsumer = new DefaultKafkaConsumer("test1", consumerProperties, handler)) {
        countDownLatch.await();
    } catch (Exception e) {
        log.error("consumer kafka record error", e);
    }
}
```

### example2

```java

@org.junit.Test
public void testReactorConsumer() throws Exception {

    CountDownLatch countDownLatch = new CountDownLatch(100);

    // kafka consumer properties
    ConsumerProperties consumerProperties = KafkaProperties.LOCAL_CONSUMER;
    consumerProperties.setTopic(Collections.singletonList("test"));
    // record handler
    Function<ConsumerRecord<String, String>, Mono<Void>> handler = stringStringConsumerRecord -> {
        countDownLatch.countDown();
        log.info("{}", stringStringConsumerRecord.value());
        return Mono.empty();
    };

    // reactor consumer
    try (DefaultReactorKafkaReceiver defaultReactorKafkaReceiver = new DefaultReactorKafkaReceiver("reactor-test1", consumerProperties, handler)) {
        countDownLatch.await();
    } catch (Exception e) {
        log.error("reactor consumer kafka record error", e);
    }
}
```

## sender

### example1

```java

@org.junit.Test
public void testSender() throws Exception {

    // sender properties
    SenderProperties senderProperties = KafkaProperties.LOCAL_SENDER;

    // sender
    try (DefaultKafkaProducer kafkaProducer = new DefaultKafkaProducer(senderProperties.getProperties())) {
        for (int i = 0; i < 100; i++) {
            kafkaProducer.send("test", i + "", (metadata, exception) -> {
                if (exception != null) {
                    log.error("send error", exception);
                } else {
                    log.info("send complete {}", metadata);
                }
            });
        }
    } catch (Exception e) {
        log.error("send error", e);
    }
}
```

### example2

```java

@org.junit.Test
public void testReactorSender() throws Exception {

    int count = 100;

    CountDownLatch countDownLatch = new CountDownLatch(count);

    // sender properties
    SenderProperties senderProperties = KafkaProperties.LOCAL_SENDER;

    // result consumer
    Consumer<SenderResult<ProducerRecord<String, String>>> senderResultConsumer = producerRecordSenderResult -> {
        countDownLatch.countDown();
        if (producerRecordSenderResult.exception() != null) {
            log.error("send error {}", producerRecordSenderResult.correlationMetadata(), producerRecordSenderResult.exception());
        } else {
            log.info("send complete {}", producerRecordSenderResult.correlationMetadata());
        }
    };

    // reactor sender
    DefaultReactorKafkaSender reactorKafkaSender = new DefaultReactorKafkaSender(senderProperties, senderResultConsumer);

    Flux.range(0, count)
            .flatMap(integer -> reactorKafkaSender.send("test", integer.toString()))
            .doOnError(throwable -> {
                log.error("send error", throwable);
            })
            .subscribe();

    countDownLatch.await();

    reactorKafkaSender.close();
}
```