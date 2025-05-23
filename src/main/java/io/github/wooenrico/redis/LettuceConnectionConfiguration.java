package io.github.wooenrico.redis;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.SslOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.resource.ClientResources;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.ssl.SslBundle;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.util.StringUtils;

public class LettuceConnectionConfiguration extends RedisConnectionConfiguration {

    public LettuceConnectionConfiguration(RedisProperties properties) {
        super(properties, null);
    }

    public LettuceConnectionConfiguration(RedisProperties properties, SslBundles sslBundles) {
        super(properties, sslBundles);
    }

    @Override
    public LettuceConnectionFactory createRedisConnectionFactory() {
        return this.createLettuceConnectionFactory(null);
    }

    public LettuceConnectionFactory createLettuceConnectionFactory(ClientResources clientResources) {

        if (clientResources == null) {
            clientResources = ClientResources.create();
        }
        LettuceClientConfiguration clientConfiguration = this.getLettuceClientConfiguration(clientResources, this.properties.getLettuce().getPool()).build();
        // sentinel
        RedisSentinelConfiguration sentinelConfig = this.getSentinelConfig();
        if (sentinelConfig != null) {
            return new LettuceConnectionFactory(sentinelConfig, clientConfiguration);
        }
        // cluster
        RedisClusterConfiguration clusterConfiguration = this.getClusterConfiguration();
        if (clusterConfiguration != null) {
            return new LettuceConnectionFactory(clusterConfiguration, clientConfiguration);
        }
        // standalone
        return new LettuceConnectionFactory(this.getStandaloneConfig(), clientConfiguration);
    }

    public LettuceClientConfiguration.LettuceClientConfigurationBuilder getLettuceClientConfiguration(ClientResources clientResources, RedisProperties.Pool pool) {

        LettuceClientConfiguration.LettuceClientConfigurationBuilder builder;
        // pool or not
        if (pool != null && pool.getEnabled() != null && pool.getEnabled()) {
            builder = LettucePoolingClientConfiguration.builder().poolConfig(this.getPoolConfig(pool));
        } else {
            builder = LettuceClientConfiguration.builder();
        }
        // properties
        if (clientResources != null) {
            builder.clientResources(clientResources);
        }
        if (this.properties.getTimeout() != null) {
            builder.commandTimeout(this.properties.getTimeout());
        }
        if (StringUtils.hasText(this.properties.getClientName())) {
            builder.clientName(this.properties.getClientName());
        }
        if (this.properties.getLettuce().getShutdownTimeout() != null && !this.properties.getLettuce().getShutdownTimeout().isZero()) {
            builder.shutdownTimeout(this.properties.getLettuce().getShutdownTimeout());
        }
        // ssl
        if (this.properties.getSsl().isEnabled()) {
            builder.useSsl();
        }
        if (StringUtils.hasText(this.properties.getUrl()) && this.urlUsesSsl()) {
            builder.useSsl();
        }
        // client options (ssl、timeout、connectTimeout、refresh)
        ClientOptions.Builder clientOptions = this.createClientOptions();
        if (clientOptions != null) {
            builder.clientOptions(clientOptions.build());
        }
        return builder;
    }

    public ClientOptions.Builder createClientOptions() {
        io.lettuce.core.ClientOptions.Builder builder = ClientOptions.builder();
        // cluster or not
        if (this.properties.getCluster() != null) {
            ClusterClientOptions.Builder cluster = ClusterClientOptions.builder();
            RedisProperties.Lettuce.Cluster.Refresh refreshProperties = this.properties.getLettuce().getCluster().getRefresh();
            ClusterTopologyRefreshOptions.Builder refreshBuilder = ClusterTopologyRefreshOptions.builder().dynamicRefreshSources(refreshProperties.isDynamicRefreshSources());
            if (refreshProperties.getPeriod() != null) {
                refreshBuilder.refreshPeriod(refreshProperties.getPeriod());
            }
            if (refreshProperties.isAdaptive()) {
                refreshBuilder.enableAllAdaptiveRefreshTriggers();
            }
            builder = cluster.topologyRefreshOptions(refreshBuilder.build());
        }
        // connectTimeout
        if (this.properties.getConnectTimeout() != null) {
            builder.socketOptions(SocketOptions.builder().connectTimeout(this.properties.getConnectTimeout()).build());
        }
        // ssl
        if (this.properties.getSsl().isEnabled() && this.properties.getSsl().getBundle() != null) {
            SslBundle sslBundle = this.sslBundles.getBundle(this.properties.getSsl().getBundle());
            io.lettuce.core.SslOptions.Builder sslOptionsBuilder = SslOptions.builder();
            sslOptionsBuilder.keyManager(sslBundle.getManagers().getKeyManagerFactory());
            sslOptionsBuilder.trustManager(sslBundle.getManagers().getTrustManagerFactory());
            org.springframework.boot.ssl.SslOptions sslOptions = sslBundle.getOptions();
            if (sslOptions.getCiphers() != null) {
                sslOptionsBuilder.cipherSuites(sslOptions.getCiphers());
            }
            if (sslOptions.getEnabledProtocols() != null) {
                sslOptionsBuilder.protocols(sslOptions.getEnabledProtocols());
            }
            builder.sslOptions(sslOptionsBuilder.build());
        }
        return builder.timeoutOptions(TimeoutOptions.enabled());
    }


    public GenericObjectPoolConfig<?> getPoolConfig(RedisProperties.Pool properties) {
        GenericObjectPoolConfig<?> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(properties.getMaxActive());
        config.setMaxIdle(properties.getMaxIdle());
        config.setMinIdle(properties.getMinIdle());
        if (properties.getTimeBetweenEvictionRuns() != null) {
            config.setTimeBetweenEvictionRuns(properties.getTimeBetweenEvictionRuns());
        }
        if (properties.getMaxWait() != null) {
            config.setMaxWait(properties.getMaxWait());
        }
        return config;
    }
}
