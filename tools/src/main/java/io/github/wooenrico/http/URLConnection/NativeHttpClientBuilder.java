package io.github.wooenrico.http.URLConnection;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.net.Proxy;

/**
 * Builder for NativeHttpClient objects.
 */
public class NativeHttpClientBuilder {
    /**
     * SSLContext
     */
    protected SSLContext sslContext;
    /**
     * HostnameVerifier
     */
    protected HostnameVerifier hostnameVerifier;
    /**
     * Proxy
     */
    protected Proxy proxy;
    /**
     * connectTimeout in milliseconds
     */
    protected int connectTimeout = 5000;
    /**
     * readTimeout in milliseconds
     */
    protected int readTimeout = 10000;

    /**
     * Sets the SSLContext for the NativeHttpClient.
     *
     * @param sslContext SSLContext
     * @return the builder
     */
    public NativeHttpClientBuilder setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
        return this;
    }

    /**
     * Sets the HostnameVerifier for the NativeHttpClient.
     *
     * @param hostnameVerifier HostnameVerifier
     * @return the builder
     */
    public NativeHttpClientBuilder setHostnameVerifier(HostnameVerifier hostnameVerifier) {
        this.hostnameVerifier = hostnameVerifier;
        return this;
    }

    /**
     * Sets the Proxy for the NativeHttpClient.
     *
     * @param proxy Proxy
     * @return the builder
     */
    public NativeHttpClientBuilder setProxy(Proxy proxy) {
        this.proxy = proxy;
        return this;
    }

    /**
     * Sets the connectTimeout for the NativeHttpClient.
     *
     * @param connectTimeout connectTimeout in milliseconds
     * @return the builder
     */
    public NativeHttpClientBuilder setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    /**
     * Sets the readTimeout for the NativeHttpClient.
     *
     * @param readTimeout readTimeout in milliseconds
     * @return the builder
     */
    public NativeHttpClientBuilder setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
        return this;
    }

    /**
     * Builds a new NativeHttpClient.
     *
     * @return a new NativeHttpClient
     */
    public NativeHttpClient build() {
        return new NativeHttpClient(this.sslContext, this.hostnameVerifier, this.proxy, this.connectTimeout, this.readTimeout);
    }
}
