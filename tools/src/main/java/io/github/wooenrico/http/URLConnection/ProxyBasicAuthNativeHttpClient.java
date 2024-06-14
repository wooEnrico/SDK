package io.github.wooenrico.http.URLConnection;

import io.github.wooenrico.http.common.HttpProxy;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;

/**
 * NativeHttpClient with Proxy Basic Auth
 */
public class ProxyBasicAuthNativeHttpClient extends NativeHttpClient {

    /**
     * Constructs a new ProxyBasicAuthNativeHttpClient.
     *
     * @param sslContext       SSLContext
     * @param hostnameVerifier HostnameVerifier
     * @param proxy            Proxy
     * @param connectTimeout   connectTimeout in milliseconds
     * @param readTimeout      readTimeout in milliseconds
     */
    public ProxyBasicAuthNativeHttpClient(SSLContext sslContext, HostnameVerifier hostnameVerifier, Proxy proxy, int connectTimeout, int readTimeout) {
        super(sslContext, hostnameVerifier, proxy, connectTimeout, readTimeout);
    }

    @Override
    public HttpURLConnection getUrlConnection(URL url) throws IOException {
        HttpURLConnection urlConnection = super.getUrlConnection(url);

        if (getProxy() == null) {
            return urlConnection;
        }

        if (getProxy() instanceof HttpProxy) {
            HttpProxy httpProxy = (HttpProxy) getProxy();
            urlConnection.setRequestProperty(HttpProxy.Proxy_Authorization_Header_Name,
                    httpProxy.getBasicAuthorization());
        }

        return urlConnection;
    }
}
