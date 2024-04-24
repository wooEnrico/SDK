package io.github.wooenrico.redis;

import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.boot.ssl.SslOptions;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.util.StringUtils;
import redis.clients.jedis.JedisPoolConfig;

import javax.net.ssl.SSLParameters;

public class JedisConnectionConfiguration extends RedisConnectionConfiguration {

    public JedisConnectionConfiguration(RedisProperties properties) {
        super(properties, null);
    }

    public JedisConnectionConfiguration(RedisProperties properties, SslBundles sslBundles) {
        super(properties, sslBundles);
    }

    @Override
    public JedisConnectionFactory createRedisConnectionFactory() {
        return this.createJedisConnectionFactory();
    }

    public JedisConnectionFactory createJedisConnectionFactory() {
        JedisClientConfiguration clientConfiguration = this.getJedisClientConfiguration();
        // sentinel
        RedisSentinelConfiguration sentinelConfig = this.getSentinelConfig();
        if (sentinelConfig != null) {
            return new JedisConnectionFactory(sentinelConfig, clientConfiguration);
        }
        // cluster
        RedisClusterConfiguration clusterConfiguration = this.getClusterConfiguration();
        if (clusterConfiguration != null) {
            return new JedisConnectionFactory(clusterConfiguration, clientConfiguration);
        }
        // standalone
        return new JedisConnectionFactory(this.getStandaloneConfig(), clientConfiguration);
    }

    private JedisClientConfiguration getJedisClientConfiguration() {
        JedisClientConfiguration.JedisClientConfigurationBuilder builder = JedisClientConfiguration.builder();
        if (this.properties.getTimeout() != null) {
            builder.readTimeout(this.properties.getTimeout());
        }
        if (this.properties.getConnectTimeout() != null) {
            builder.connectTimeout(this.properties.getConnectTimeout());
        }
        if (this.properties.getClientName() != null) {
            builder.clientName(this.properties.getClientName());
        }
        // pool
        if (this.properties.getJedis() != null) {
            RedisProperties.Pool pool = this.properties.getJedis().getPool();
            if (pool != null && pool.getEnabled() != null && pool.getEnabled()) {
                builder.usePooling().poolConfig(this.jedisPoolConfig(pool));
            }
        }
        // ssl
        if (this.properties.getSsl().isEnabled()) {
            JedisClientConfiguration.JedisSslClientConfigurationBuilder sslBuilder = builder.useSsl();
            if (this.properties.getSsl().getBundle() != null) {
                SslBundle sslBundle = this.sslBundles.getBundle(this.properties.getSsl().getBundle());
                sslBuilder.sslSocketFactory(sslBundle.createSslContext().getSocketFactory());
                SslOptions sslOptions = sslBundle.getOptions();
                SSLParameters sslParameters = new SSLParameters();

                if (sslOptions.getCiphers() != null) {
                    sslParameters.setCipherSuites(sslOptions.getCiphers());
                }
                if (sslOptions.getEnabledProtocols() != null) {
                    sslParameters.setProtocols(sslOptions.getEnabledProtocols());
                }
                sslBuilder.sslParameters(sslParameters);
            }
        }
        if (StringUtils.hasText(this.properties.getUrl()) && this.urlUsesSsl()) {
            builder.useSsl();
        }
        return builder.build();
    }

    private JedisPoolConfig jedisPoolConfig(RedisProperties.Pool pool) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(pool.getMaxActive());
        config.setMaxIdle(pool.getMaxIdle());
        config.setMinIdle(pool.getMinIdle());
        if (pool.getTimeBetweenEvictionRuns() != null) {
            config.setTimeBetweenEvictionRuns(pool.getTimeBetweenEvictionRuns());
        }
        if (pool.getMaxWait() != null) {
            config.setMaxWait(pool.getMaxWait());
        }

        return config;
    }
}
