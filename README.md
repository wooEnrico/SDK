# USEAGE

java version >= 8
spring boot version = 2.7.5

## dependency

```xml
<dependencies>
    <dependency>
        <groupId>io.github.wooenrico</groupId>
        <artifactId>spring-boot-data-redis</artifactId>
        <version>2.7.5.1</version>
    </dependency>
</dependencies>
```

## define

```java

import io.github.wooenrico.redis.LettuceConnectionConfiguration;
import io.github.wooenrico.redis.JedisConnectionConfiguration;

@Configuration
public class RedisConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "redis.test")
    public RedisProperties testRedisProperties() {
        return new RedisProperties();
    }

    @Bean
    public RedisConnectionFactory testRedisConnectionFactory(@Qualifier("testRedisProperties") RedisProperties redisProperties) {
        return RedisProperties.ClientType.LETTUCE.equals(redisProperties.getClientType()) ?
                new LettuceConnectionConfiguration(redisProperties).createRedisConnectionFactory()
                : new JedisConnectionConfiguration(redisProperties).createRedisConnectionFactory();
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
    public LettuceConnectionFactory test2RedisConnectionFactory(@Qualifier("test2RedisProperties") RedisProperties redisProperties) {
        return new LettuceConnectionConfiguration(redisProperties).createRedisConnectionFactory();
    }

    @Bean
    public ReactiveStringRedisTemplate test2ReactiveStringRedisTemplate(@Qualifier("test2RedisConnectionFactory") LettuceConnectionFactory redisConnectionFactory) {
        return new ReactiveStringRedisTemplate(redisConnectionFactory);
    }
}
```

## if springframework default is unuseful then exclude

```java

@SpringBootApplication(exclude = {RedisAutoConfiguration.class, RedisRepositoriesAutoConfiguration.class})
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
########################################################################################################################
## TODO END redis configuration
########################################################################################################################
```