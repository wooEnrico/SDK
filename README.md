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
