package io.github.wooenrico.kafka.sinks;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SinksManyCacheTest {

    @Test
    void getReturnsQuicklyAfterSubscriberIsAttached() {
        TestSinksManyCache cache = new TestSinksManyCache(Duration.ofSeconds(2));
        cache.build();

        long start = System.nanoTime();
        Sinks.Many<Integer> sink = cache.get("key");
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - start).toMillis();

        assertEquals(1, sink.currentSubscriberCount());
        assertTrue(elapsedMillis < 500, "cache get should return quickly once subscriber is attached, took " + elapsedMillis + "ms");
    }

    @Test
    void emittedValuesReachResultConsumerAfterSubscriberIsAttached() {
        TestSinksManyCache cache = new TestSinksManyCache(Duration.ofSeconds(2));
        cache.build();

        Sinks.Many<Integer> sink = cache.get("key");
        Sinks.EmitResult result = sink.tryEmitNext(1);

        assertTrue(result.isSuccess());
        assertEquals(Collections.singletonList(1), cache.results);
    }

    @Test
    void disposeCompletesAllSinks() {
        TestSinksManyCache cache = new TestSinksManyCache(Duration.ofSeconds(2));
        cache.build();

        Sinks.Many<Integer> sink = cache.get("key");
        cache.dispose();

        // After dispose, emit should fail because sink is completed
        Sinks.EmitResult result = sink.tryEmitNext(2);
        assertTrue(result.isFailure(), "emit should fail after dispose");
    }

    private static class TestSinksManyCache extends SinksManyCache<String, Integer, Integer> {
        final List<Integer> results = new CopyOnWriteArrayList<>();
        private final Duration await;

        private TestSinksManyCache(Duration await) {
            this.await = await;
        }

        @Override
        protected Flux<Integer> flatMap(String key, Flux<Integer> flux) {
            return flux;
        }

        @Override
        protected void result(String key, Integer result) {
            this.results.add(result);
        }

        @Override
        protected Duration getSinksSubscribeAwait() {
            return this.await;
        }
    }
}
