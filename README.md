# USEAGE

java version >= 17
spring boot version = 3.1.2

## dependency

```xml
<dependencies>
    <dependency>
        <groupId>io.github.wooenrico</groupId>
        <artifactId>spring-boot-data-redis</artifactId>
        <version>3.1.2.1</version>
    </dependency>
</dependencies>
```

## define

```java

import io.github.wooenrico.redis.LettuceConnectionConfiguration;
import io.github.wooenrico.redis.JedisConnectionConfiguration;

@Configuration
public class AutoRedisConfiguration {


    @Bean
    @ConfigurationProperties(prefix = "redis.test")
    public RedisProperties testRedisProperties() {
        return new RedisProperties();
    }

    @Bean
    public RedisConnectionFactory testRedisConnectionFactory(@Qualifier("testRedisProperties") RedisProperties redisProperties, ObjectProvider<SslBundles> sslBundles) {
        return RedisProperties.ClientType.LETTUCE.equals(redisProperties.getClientType()) ?
                new LettuceConnectionConfiguration(redisProperties, sslBundles.getIfAvailable()).createRedisConnectionFactory()
                : new JedisConnectionConfiguration(redisProperties, sslBundles.getIfAvailable()).createRedisConnectionFactory();
    }

    @Bean
    public StringRedisTemplate testStringRedisTemplate(@Qualifier("testRedisConnectionFactory") RedisConnectionFactory redisConnectionFactory) {
        return new StringRedisTemplate(redisConnectionFactory);
    }

    @Bean
    @ConfigurationProperties(prefix = "redis.test2")
    public RedisProperties test2PopRedisProperties() {
        return new RedisProperties();
    }

    @Bean
    public LettuceConnectionFactory test2RedisConnectionFactory(@Qualifier("test2RedisProperties") RedisProperties redisProperties, ObjectProvider<SslBundles> sslBundles) {
        return new LettuceConnectionConfiguration(redisProperties, sslBundles.getIfAvailable()).createRedisConnectionFactory();
    }

    @Bean
    public ReactiveStringRedisTemplate test2ReactiveStringRedisTemplate(@Qualifier("test2RedisConnectionFactory") LettuceConnectionFactory redisConnectionFactory) {
        return new ReactiveStringRedisTemplate(redisConnectionFactory);
    }
}
```

## if springframework default is unuseful then exclude

```java

@SpringBootApplication(exclude = {RedisAutoConfiguration.class, RedisReactiveAutoConfiguration.class, RedisRepositoriesAutoConfiguration.class})
public class MyApplication {
    public static void main(String[] args) {
        new SpringApplicationBuilder(MyApplication.class)
                .web(WebApplicationType.NONE)
                .run(args);
    }
}
```

## properties

```properties
########################################################################################################################
## TODO START redis configuration
########################################################################################################################
## example-1
redis.test1.host=127.0.0.1
redis.test1.port=6379
redis.test1.password=
## example-1
redis.test2.cluster.nodes=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
redis.test2.password=
redis.test2.clientType=LETTUCE
redis.test2.ssl.enabled=true
redis.test2.ssl.bundle=mybundle
########################################################################################################################
## TODO END redis configuration
########################################################################################################################
## spring boot 3.0 SSL
## https://docs.spring.io/spring-boot/docs/current/reference/htmlsingle/#features.ssl
spring.ssl.bundle.jks.mybundle.keystore.location=classpath:application.p12
spring.ssl.bundle.jks.mybundle.keystore.password=secret
spring.ssl.bundle.jks.mybundle.keystore.type=PKCS12
spring.ssl.bundle.jks.mybundle.truststore.location=classpath:server.p12
spring.ssl.bundle.jks.mybundle.truststore.password=secret
spring.ssl.bundle.pem.mybundle2.keystore.certificate=classpath:application.crt
spring.ssl.bundle.pem.mybundle2.keystore.private-key=classpath:application.key
spring.ssl.bundle.pem.mybundle2.truststore.certificate=classpath:server.crt
```