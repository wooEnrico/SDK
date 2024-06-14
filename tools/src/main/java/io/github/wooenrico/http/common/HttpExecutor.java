package io.github.wooenrico.http.common;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * HttpExecutor
 */
public interface HttpExecutor {
    /**
     * 执行http请求
     *
     * @param httpRequest http请求内容
     * @return HttpResponse http响应
     * @throws Exception 异常
     */
    HttpResponse execute(HttpRequest httpRequest) throws Exception;

    /**
     * 异步执行http请求
     *
     * @param httpRequest http请求内容
     * @param executor    线程池
     * @return CompletableFuture of HttpResponse http响应
     */
    CompletableFuture<HttpResponse> execute(HttpRequest httpRequest, Executor executor);
}
