package io.github.wooenrico.http.common;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * HttpResponse Object
 */
public class HttpResponse {
    private final int status;
    private final String desc;
    private final String version;
    private final Map<String, List<String>> headers;
    private final byte[] body;

    /**
     * Constructs a new HttpResponse.
     *
     * @param status  the status for the response
     * @param desc    the desc for the response
     * @param version the version for the response
     * @param headers the headers for the response
     * @param body    the body of the response
     */
    public HttpResponse(int status, String desc, String version, Map<String, List<String>> headers, byte[] body) {
        this.status = status;
        this.desc = desc;
        this.version = version;
        this.headers = headers;
        this.body = body;
    }

    /**
     * Returns the status of the response.
     *
     * @return the status of the response
     */
    public int getStatus() {
        return status;
    }

    /**
     * Returns the desc of the response.
     *
     * @return the desc of the response
     */
    public String getDesc() {
        return desc;
    }

    /**
     * Returns the version of the response.
     *
     * @return the version of the response
     */
    public String getVersion() {
        return version;
    }

    /**
     * Returns the headers of the response.
     *
     * @return the headers of the response
     */
    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    /**
     * Returns the body of the response.
     *
     * @return the body of the response
     */
    public byte[] getBody() {
        return body;
    }

    /**
     * Returns the response as a pretty string.
     *
     * @return the response as a pretty string
     */
    public String prettyString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.version == null ? null : this.version.toUpperCase(Locale.ROOT))
                .append(" ")
                .append(this.status)
                .append(" ")
                .append(this.desc)
                .append("\r\n");
        if (this.headers != null) {
            this.headers.forEach((k, v) -> {
                stringBuilder.append(k)
                        .append(":")
                        .append(v.toString())
                        .append("\r\n");
            });
        }
        stringBuilder.append("\r\n");

        if (this.body != null) {
            stringBuilder.append(new String(this.body, StandardCharsets.UTF_8));
        }
        return stringBuilder.toString();
    }
}
