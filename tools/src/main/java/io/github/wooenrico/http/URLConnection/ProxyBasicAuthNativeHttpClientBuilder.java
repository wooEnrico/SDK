package io.github.wooenrico.http.URLConnection;

/**
 * Builder for ProxyBasicAuthNativeHttpClient objects.
 */
public class ProxyBasicAuthNativeHttpClientBuilder extends NativeHttpClientBuilder {
    @Override
    public ProxyBasicAuthNativeHttpClient build() {
        return new ProxyBasicAuthNativeHttpClient(sslContext, hostnameVerifier, proxy, connectTimeout, readTimeout);
    }
}
