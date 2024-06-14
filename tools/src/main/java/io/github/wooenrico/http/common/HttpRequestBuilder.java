package io.github.wooenrico.http.common;

import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Builder for HttpRequest objects.
 */
public class HttpRequestBuilder {
    private String url;
    private Map<String, String> urlParam = new HashMap<>();
    private Method method;
    private Map<String, String> headers = new HashMap<>();
    private byte[] body;


    /**
     * Sets the URL for the HttpRequest.
     *
     * @param url the URL
     * @return the builder
     */
    public HttpRequestBuilder url(String url) {
        this.url = url;
        return this;
    }

    /**
     * Sets the URL parameter for the HttpRequest.
     *
     * @param key   header key
     * @param value header value
     * @return the builder
     */
    public HttpRequestBuilder urlParam(String key, String value) {
        this.urlParam.put(key, value);
        return this;
    }

    /**
     * Sets the HTTP method for the HttpRequest.
     *
     * @param method the HTTP method
     * @return the builder
     */
    public HttpRequestBuilder method(Method method) {
        this.method = method;
        return this;
    }

    /**
     * Sets the body for the HttpRequest.
     *
     * @param body the body bytes
     * @return the builder
     */
    public HttpRequestBuilder body(byte[] body) {
        this.body = body;
        return this;
    }

    /**
     * Sets the headers for the HttpRequest.
     *
     * @param headers the headers map
     * @return the builder
     */
    public HttpRequestBuilder headers(Map<String, String> headers) {
        this.headers.putAll(headers);
        return this;
    }

    /**
     * Adds a header to the HttpRequest.
     *
     * @param key   header key
     * @param value header value
     * @return the builder
     */
    public HttpRequestBuilder header(String key, String value) {
        this.headers.put(key, value);
        return this;
    }

    /**
     * Sets the header for the HttpRequest.
     *
     * @param isKeepAlive is keep alive
     * @return the builder
     */
    public HttpRequestBuilder keepAlive(boolean isKeepAlive) {
        return header("Connection", isKeepAlive ? "Keep-Alive" : "Close");
    }

    /**
     * Builds the HttpRequest.
     *
     * @return the HttpRequest
     */
    public HttpRequest build() {
        if (url == null) {
            throw new RuntimeException("url null");
        }
        if (method == null) {
            throw new RuntimeException("method null");
        }

        if (urlParam.isEmpty()) {
            return new HttpRequest(this.url, method, headers, body);
        }

        StringBuilder stringBuilder = new StringBuilder(url);
        stringBuilder.append("?");

        for (Map.Entry<String, String> stringStringEntry : urlParam.entrySet()) {
            String encode = URLEncoder.encode(stringStringEntry.getValue());
            stringBuilder.append(stringStringEntry.getKey()).append("=").append(encode).append("&");
        }

        String substring = stringBuilder.substring(0, stringBuilder.length() - 1);

        return new HttpRequest(substring, method, headers, body);

    }
}
