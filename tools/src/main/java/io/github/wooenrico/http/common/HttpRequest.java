package io.github.wooenrico.http.common;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * HttpRequest Object
 */
public class HttpRequest {
    private final String url;
    private final Method method;
    private final Map<String, String> headers;
    private final byte[] body;

    /**
     * Constructs a new HttpRequest.
     *
     * @param url     the URL for the request
     * @param method  the HTTP method for the request
     * @param headers the headers for the request
     * @param body    the body of the request
     */
    public HttpRequest(String url, Method method, Map<String, String> headers, byte[] body) {
        this.url = url;
        this.method = method;
        this.headers = headers;
        this.body = body;
    }

    /**
     * Returns the URL of the request.
     *
     * @return the URL of the request
     */
    public String getUrl() {
        return url;
    }

    /**
     * Returns the HTTP method of the request.
     *
     * @return the HTTP method of the request
     */
    public Method getMethod() {
        return method;
    }

    /**
     * Returns the headers of the request.
     *
     * @return the headers of the request
     */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * Returns the body of the request.
     *
     * @return the body of the request
     */
    public byte[] getBody() {
        return body;
    }

    /**
     * Returns a string representation of the HTTP request.
     *
     * @return a string representation of the HTTP request
     */
    @Override
    public String toString() {
        return prettyString();
    }

    /**
     * Returns a pretty string representation of the HTTP request.
     *
     * @return a pretty string representation of the HTTP request
     */
    public String prettyString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.method.name())
                .append(" ")
                .append(this.getUrl())
                .append(" ")
                .append("\r\n");
        if (this.headers != null) {
            for (Map.Entry<String, String> stringStringEntry : this.headers.entrySet()) {
                stringBuilder.append(stringStringEntry.getKey())
                        .append(":")
                        .append(stringStringEntry.getValue())
                        .append("\r\n");
            }
        }

        stringBuilder.append("\r\n");
        if (body != null) {
            stringBuilder.append(new String(body, StandardCharsets.UTF_8));
        }
        return stringBuilder.toString();
    }
}



