package io.github.wooenrico.redis;

import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.data.redis.connection.*;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

public abstract class RedisConnectionConfiguration {

    protected final RedisProperties properties;
    protected final SslBundles sslBundles;

    public RedisConnectionConfiguration(RedisProperties properties, SslBundles sslBundles) {
        this.properties = properties;
        this.sslBundles = sslBundles;
    }

    protected final RedisStandaloneConfiguration getStandaloneConfig() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        if (StringUtils.hasText(this.properties.getUrl())) {
            RedisConnectionInfo connectionInfo = RedisConnectionInfo.parseUrl(this.properties.getUrl());
            config.setHostName(connectionInfo.getUri().getHost());
            config.setPort(connectionInfo.getUri().getPort());
            config.setUsername(connectionInfo.getUsername());
            config.setPassword(RedisPassword.of(connectionInfo.getPassword()));
        } else {
            config.setHostName(this.properties.getHost());
            config.setPort(this.properties.getPort());
            config.setUsername(this.properties.getUsername());
            config.setPassword(RedisPassword.of(this.properties.getPassword()));
        }

        config.setDatabase(this.properties.getDatabase());
        return config;
    }

    protected final RedisSentinelConfiguration getSentinelConfig() {
        RedisProperties.Sentinel sentinelProperties = this.properties.getSentinel();
        if (sentinelProperties == null) {
            return null;
        }

        RedisSentinelConfiguration config = new RedisSentinelConfiguration();
        config.master(sentinelProperties.getMaster());

        List<RedisNode> nodeList = sentinelProperties.getNodes().stream().map(RedisNode::fromString).collect(Collectors.toList());

        config.setSentinels(nodeList);
        config.setUsername(this.properties.getUsername());
        if (this.properties.getPassword() != null) {
            config.setPassword(RedisPassword.of(this.properties.getPassword()));
        }

        config.setSentinelUsername(sentinelProperties.getUsername());
        if (sentinelProperties.getPassword() != null) {
            config.setSentinelPassword(RedisPassword.of(sentinelProperties.getPassword()));
        }

        config.setDatabase(this.properties.getDatabase());
        return config;
    }

    protected final RedisClusterConfiguration getClusterConfiguration() {
        if (this.properties.getCluster() == null) {
            return null;
        }

        RedisProperties.Cluster clusterProperties = this.properties.getCluster();
        RedisClusterConfiguration config = new RedisClusterConfiguration(clusterProperties.getNodes());
        if (clusterProperties.getMaxRedirects() != null) {
            config.setMaxRedirects(clusterProperties.getMaxRedirects());
        }

        config.setUsername(this.properties.getUsername());
        if (this.properties.getPassword() != null) {
            config.setPassword(RedisPassword.of(this.properties.getPassword()));
        }

        return config;
    }

    protected final boolean urlUsesSsl() {
        return RedisConnectionInfo.parseUrl(this.properties.getUrl()).isUseSsl();
    }

    protected abstract RedisConnectionFactory createRedisConnectionFactory();
}
