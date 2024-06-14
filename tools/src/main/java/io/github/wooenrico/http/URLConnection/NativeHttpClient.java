package io.github.wooenrico.http.URLConnection;


import io.github.wooenrico.InputStreamUtil;
import io.github.wooenrico.http.common.HttpExecutor;
import io.github.wooenrico.http.common.HttpRequest;
import io.github.wooenrico.http.common.HttpResponse;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * NativeHttpClient
 */
public class NativeHttpClient implements HttpExecutor {
    private final SSLContext sslContext;
    private final HostnameVerifier hostnameVerifier;
    private final Proxy proxy;
    private final int connectTimeout;
    private final int readTimeout;

    /**
     * Constructs a new NativeHttpClient.
     *
     * @param sslContext       SSLContext
     * @param hostnameVerifier HostnameVerifier
     * @param proxy            Proxy
     * @param connectTimeout   connectTimeout in milliseconds
     * @param readTimeout      readTimeout in milliseconds
     */
    public NativeHttpClient(SSLContext sslContext, HostnameVerifier hostnameVerifier, Proxy proxy, int connectTimeout, int readTimeout) {
        this.sslContext = sslContext;
        this.hostnameVerifier = hostnameVerifier;
        this.proxy = proxy;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
    }

    /**
     * Returns the SSLContext of the client.
     *
     * @return the SSLContext of the client
     */
    public SSLContext getSslContext() {
        return sslContext;
    }

    /**
     * Returns the HostnameVerifier of the client.
     *
     * @return the HostnameVerifier of the client
     */
    public HostnameVerifier getHostnameVerifier() {
        return hostnameVerifier;
    }

    /**
     * Returns the Proxy of the client.
     *
     * @return the Proxy of the client
     */
    public Proxy getProxy() {
        return proxy;
    }

    /**
     * Returns the connectTimeout of the client.
     *
     * @return the connectTimeout of the client
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Returns the readTimeout of the client.
     *
     * @return the readTimeout of the client
     */
    public int getReadTimeout() {
        return readTimeout;
    }

    /**
     * Returns the HttpURLConnection of the URL.
     *
     * @param url URL
     * @return the HttpURLConnection of the URL
     * @throws IOException if an I/O error occurs
     */
    public HttpURLConnection getUrlConnection(URL url) throws IOException {
        URLConnection connection =
                this.proxy == null ? url.openConnection() : url.openConnection(this.proxy);

        if (connection instanceof HttpsURLConnection) {
            if (this.sslContext != null) {
                ((HttpsURLConnection) connection)
                        .setSSLSocketFactory(this.sslContext.getSocketFactory());
            }
            if (this.hostnameVerifier != null) {
                ((HttpsURLConnection) connection).setHostnameVerifier(this.hostnameVerifier);
            }
        }

        return (HttpURLConnection) connection;
    }

    @Override
    public HttpResponse execute(HttpRequest httpRequest) throws IOException {
        return doRequest(new URL(httpRequest.getUrl()), httpRequest.getMethod().name(), httpRequest.getHeaders(), httpRequest.getBody());
    }

    @Override
    public CompletableFuture<HttpResponse> execute(HttpRequest httpRequest, Executor executor) {
        CompletableFuture<HttpResponse> future = new CompletableFuture<>();
        try {
            if (executor == null) {
                executor = Runnable::run;
            }

            executor.execute(() -> {
                try {
                    HttpResponse httpResponse = execute(httpRequest);
                    future.complete(httpResponse);
                } catch (Exception e) {
                    future.completeExceptionally(e);
                }
            });
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    private HttpResponse doRequest(URL url, String requestMethod, Map<String, String> headers, byte[] body) throws IOException {

        HttpURLConnection connection = getUrlConnection(url);
        connection.setConnectTimeout(this.connectTimeout);
        connection.setReadTimeout(this.readTimeout);
        connection.setDoInput(true);
        connection.setRequestMethod(requestMethod);

        if (headers != null) {
            headers.forEach(connection::setRequestProperty);
        }

        if (body != null && body.length > 0) {
            connection.setDoOutput(true);
            connection.setFixedLengthStreamingMode(body.length);

            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(body);
            outputStream.flush();
        }

        int responseCode = connection.getResponseCode();
        String desc = "";
        String version = "";

        // HTTP/1.1 404 Not Found
        // HTTP/1.0 200
        String statusLine = connection.getHeaderField(0);

        if (statusLine.startsWith("HTTP/1.")) {

            int codeIndex = statusLine.indexOf(' ');

            version = statusLine.substring(0, codeIndex);

            if (codeIndex > 0) {
                int phraseIndex = statusLine.indexOf(' ', codeIndex + 1);
                if (phraseIndex > 0) {
                    desc = statusLine.substring(phraseIndex + 1);
                }

                if (phraseIndex < 0)
                    phraseIndex = statusLine.length();
                try {
                    responseCode =
                            Integer.parseInt(statusLine.substring(codeIndex + 1, phraseIndex));
                } catch (NumberFormatException ignore) {
                }
            }
        }

        Map<String, List<String>> headerFields = connection.getHeaderFields();
        Map<String, List<String>> headerMap = new HashMap<>();
        for (Map.Entry<String, List<String>> stringListEntry : headerFields.entrySet()) {
            String key = stringListEntry.getKey();
            if (key != null) {
                headerMap.put(key, stringListEntry.getValue());
            }
        }

        InputStream inputStream;
        try {
            inputStream = connection.getInputStream();
        } catch (IOException e) {
            inputStream = connection.getErrorStream();
        }

        byte[] bytes = InputStreamUtil.readAndClose(inputStream);

        return new HttpResponse(responseCode, desc, version, headerMap, bytes);
    }
}
