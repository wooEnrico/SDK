package io.github.wooenrico.redis;

import org.springframework.boot.data.redis.autoconfigure.DataRedisProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.data.redis.connection.*;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

public abstract class RedisConnectionConfiguration {

    protected final DataRedisProperties properties;
    protected final SslBundles sslBundles;

    public RedisConnectionConfiguration(DataRedisProperties properties, SslBundles sslBundles) {
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
        DataRedisProperties.Sentinel sentinelProperties = this.properties.getSentinel();
        if (sentinelProperties == null || sentinelProperties.getNodes() == null) {
            return null;
        }

        List<RedisNode> nodeList = sentinelProperties.getNodes().stream().map(RedisNode::fromString).collect(Collectors.toList());
        if (nodeList.isEmpty()) {
            return null;
        }
        RedisSentinelConfiguration config = new RedisSentinelConfiguration();
        if (sentinelProperties.getMaster() != null) {
            config.master(sentinelProperties.getMaster());
        }
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
        if (this.properties.getCluster() == null || this.properties.getCluster().getNodes() == null) {
            return null;
        }

        DataRedisProperties.Cluster clusterProperties = this.properties.getCluster();
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

    protected final RedisStaticMasterReplicaConfiguration getMasterReplicaConfiguration() {
        if (this.properties.getMasterreplica() == null || this.properties.getMasterreplica().getNodes() == null) {
            return null;
        }

        List<RedisNode> nodes = this.properties.getMasterreplica().getNodes().stream().map(RedisNode::fromString).toList();
        if (nodes.isEmpty()) {
            return null;
        }
        RedisNode redisNode = nodes.getFirst();
        if (redisNode == null || redisNode.getHost() == null || redisNode.getPort() == null) {
            return null;
        }
        RedisStaticMasterReplicaConfiguration config = new RedisStaticMasterReplicaConfiguration(redisNode.getHost(), redisNode.getPort());
        nodes.stream().skip(1L).forEach(node -> {
            if (node.getHost() != null && node.getPort() != null) {
                config.addNode(node.getHost(), node.getPort());
            }
        });
        config.setUsername(this.properties.getUsername());
        String password = this.properties.getPassword();
        if (password != null) {
            config.setPassword(RedisPassword.of(password));
        }

        return config;
    }

    protected final boolean urlUsesSsl() {
        return RedisConnectionInfo.parseUrl(this.properties.getUrl()).isUseSsl();
    }

    protected abstract RedisConnectionFactory createRedisConnectionFactory();
}
