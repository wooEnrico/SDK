# USEAGE

## version

![GitHub tag (latest by date)](https://img.shields.io/github/v/tag/wooEnrico/SDK)

## dependency

you can use in spring boot 2.x

```xml

<dependencies>
    <dependency>
        <groupId>io.github.wooenrico</groupId>
        <artifactId>spring-boot-kafka-starter</artifactId>
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
