import io.github.wooenrico.redis.JedisConnectionConfiguration;
import io.github.wooenrico.redis.LettuceConnectionConfiguration;
import org.junit.Ignore;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Arrays;
import java.util.Objects;

public class TestRedisTemplate {

    @Ignore
    @org.junit.Test
    public void testStandalone() {
        RedisProperties redisProperties = new RedisProperties();
        JedisConnectionConfiguration jedisConnectionConfiguration = new JedisConnectionConfiguration(redisProperties);

        JedisConnectionFactory jedisConnectionFactory = jedisConnectionConfiguration.createRedisConnectionFactory();
        jedisConnectionFactory.afterPropertiesSet();

        StringRedisTemplate stringRedisTemplate = new StringRedisTemplate(jedisConnectionFactory);

        stringRedisTemplate.opsForValue().set("key", "value");
        String test = stringRedisTemplate.opsForValue().get("key");
        Boolean key = stringRedisTemplate.delete("key");

        assert Objects.equals(test, "value");
        assert Boolean.TRUE.equals(key);
    }

    @org.junit.Test
    @Ignore
    public void testCluster() {
        RedisProperties redisProperties = new RedisProperties();
        RedisProperties.Cluster cluster = new RedisProperties.Cluster();
        cluster.setNodes(Arrays.asList("localhost:7001", "localhost:7002", "localhost:7003"));
        redisProperties.setCluster(cluster);
        LettuceConnectionConfiguration lettuceConnectionConfiguration = new LettuceConnectionConfiguration(redisProperties);

        LettuceConnectionFactory lettuceConnectionFactory = lettuceConnectionConfiguration.createRedisConnectionFactory();
        lettuceConnectionFactory.afterPropertiesSet();

        StringRedisTemplate stringRedisTemplate = new StringRedisTemplate(lettuceConnectionFactory);

        stringRedisTemplate.opsForValue().set("key", "value");
        String test = stringRedisTemplate.opsForValue().get("key");
        Boolean key = stringRedisTemplate.delete("key");

        assert Objects.equals(test, "value");
        assert Boolean.TRUE.equals(key);
    }
}
