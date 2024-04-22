package io.github.wooenrico.redis;

import java.net.URI;
import java.net.URISyntaxException;

public class RedisConnectionInfo {

    private final static String REDIS_SCHEME = "redis";
    private final static String REDIS_SECURE_SCHEME = "rediss";

    private final URI uri;
    private final boolean useSsl;
    private final String username;
    private final String password;

    public RedisConnectionInfo(URI uri, boolean useSsl, String username, String password) {
        this.uri = uri;
        this.useSsl = useSsl;
        this.username = username;
        this.password = password;
    }

    URI getUri() {
        return this.uri;
    }

    boolean isUseSsl() {
        return this.useSsl;
    }

    String getUsername() {
        return this.username;
    }

    String getPassword() {
        return this.password;
    }

    public static RedisConnectionInfo parseUrl(String url) {
        try {
            URI uri = new URI(url);
            return getRedisConnectionInfo(uri);
        } catch (URISyntaxException e) {
            throw new RedisUrlSyntaxException(url, e);
        }
    }

    private static RedisConnectionInfo getRedisConnectionInfo(URI uri) {
        String scheme = uri.getScheme();
        if (REDIS_SCHEME.equals(scheme) || REDIS_SECURE_SCHEME.equals(scheme)) {
            boolean useSsl = REDIS_SECURE_SCHEME.equals(scheme);
            String username = null;
            String password = null;
            if (uri.getUserInfo() != null) {
                String userInfo = uri.getUserInfo();
                int index = userInfo.indexOf(":");
                if (index >= 0) {
                    username = userInfo.substring(0, index);
                    password = userInfo.substring(index + 1);
                } else {
                    password = userInfo;
                }
            }
            return new RedisConnectionInfo(uri, useSsl, username, password);
        }
        throw new RedisUrlSyntaxException(uri.toString());
    }

    static class RedisUrlSyntaxException extends RuntimeException {
        RedisUrlSyntaxException(String url) {
            super(buildMessage(url));
        }

        public RedisUrlSyntaxException(String url, Throwable cause) {
            super(buildMessage(url), cause);
        }

        private static String buildMessage(String url) {
            return "Invalid Redis URL '" + url + "'";
        }
    }
}
