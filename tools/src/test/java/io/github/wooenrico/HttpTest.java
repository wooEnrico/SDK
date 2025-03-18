package io.github.wooenrico;

import io.github.wooenrico.http.URLConnection.NativeHttpClient;
import io.github.wooenrico.http.URLConnection.NativeHttpClientBuilder;
import io.github.wooenrico.http.common.HttpRequest;
import io.github.wooenrico.http.common.HttpRequestBuilder;
import io.github.wooenrico.http.common.HttpResponse;
import io.github.wooenrico.http.common.Method;
import org.junit.jupiter.api.Test;


import java.io.IOException;

public class HttpTest {

    @Test
    public void testNativeHttpClient() throws IOException {
        NativeHttpClient nativeHttpClient = new NativeHttpClientBuilder().build();

        HttpRequest httpRequest = new HttpRequestBuilder()
                .method(Method.GET)
                .url("https://github.com/wooEnrico/SDK")
                .build();

        HttpResponse execute = nativeHttpClient.execute(httpRequest);

        assert execute.getStatus() == 200;

        HttpResponse join = nativeHttpClient.execute(httpRequest, Runnable::run).join();

        assert join.getStatus() == 200;
    }
}
