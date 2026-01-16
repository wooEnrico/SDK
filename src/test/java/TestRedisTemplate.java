
import io.github.wooenrico.redis.JedisConnectionConfiguration;
import io.github.wooenrico.redis.LettuceConnectionConfiguration;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.data.redis.autoconfigure.DataRedisProperties;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestRedisTemplate {

    @Disabled
    @Test
    public void testStandalone() {
        DataRedisProperties redisProperties = new DataRedisProperties();
        JedisConnectionConfiguration jedisConnectionConfiguration = new JedisConnectionConfiguration(redisProperties);

        JedisConnectionFactory jedisConnectionFactory = jedisConnectionConfiguration.createRedisConnectionFactory();
        jedisConnectionFactory.afterPropertiesSet();

        StringRedisTemplate stringRedisTemplate = new StringRedisTemplate(jedisConnectionFactory);

        stringRedisTemplate.opsForValue().set("key", "value");
        String test = stringRedisTemplate.opsForValue().get("key");
        Boolean key = stringRedisTemplate.delete("key");

        assertEquals("value", test);
        assertEquals(Boolean.TRUE, key);
    }

    @Test
    @Disabled
    public void testCluster() {
        DataRedisProperties redisProperties = new DataRedisProperties();
        DataRedisProperties.Cluster cluster = new DataRedisProperties.Cluster();
        cluster.setNodes(Arrays.asList("localhost:7001", "localhost:7002", "localhost:7003"));
        redisProperties.setCluster(cluster);
        LettuceConnectionConfiguration lettuceConnectionConfiguration = new LettuceConnectionConfiguration(redisProperties);

        LettuceConnectionFactory lettuceConnectionFactory = lettuceConnectionConfiguration.createRedisConnectionFactory();
        lettuceConnectionFactory.afterPropertiesSet();

        StringRedisTemplate stringRedisTemplate = new StringRedisTemplate(lettuceConnectionFactory);

        stringRedisTemplate.opsForValue().set("key", "value");
        String test = stringRedisTemplate.opsForValue().get("key");
        Boolean key = stringRedisTemplate.delete("key");

        assertEquals("value", test);
        assertEquals(Boolean.TRUE, key);
    }

    @Test
    @Disabled
    public void testMaster() {
        DataRedisProperties redisProperties = new DataRedisProperties();
        DataRedisProperties.Masterreplica masterreplica = new DataRedisProperties.Masterreplica();
        masterreplica.setNodes(Arrays.asList("localhost:6378", "localhost:6380"));
        redisProperties.setMasterreplica(masterreplica);
        LettuceConnectionConfiguration lettuceConnectionConfiguration = new LettuceConnectionConfiguration(redisProperties);

        LettuceConnectionFactory lettuceConnectionFactory = lettuceConnectionConfiguration.createRedisConnectionFactory();
        lettuceConnectionFactory.afterPropertiesSet();

        StringRedisTemplate stringRedisTemplate = new StringRedisTemplate(lettuceConnectionFactory);

        stringRedisTemplate.opsForValue().set("key", "value");
        String test = stringRedisTemplate.opsForValue().get("key");
        Boolean key = stringRedisTemplate.delete("key");

        assertEquals("value", test);
        assertEquals(Boolean.TRUE, key);
    }
}
