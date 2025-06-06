# USEAGE

## version

![GitHub tag (latest by date)](https://img.shields.io/github/v/tag/wooEnrico/SDK)

## dependency

```xml

<dependencies>
    <dependency>
        <groupId>io.github.wooenrico</groupId>
        <artifactId>kafka</artifactId>
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

## consumer

https://github.com/wooEnrico/SDK/blob/master/kafka/src/test/java/TestConsumer.java

## sender

https://github.com/wooEnrico/SDK/blob/master/kafka/src/test/java/TestSender.java
