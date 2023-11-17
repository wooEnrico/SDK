import org.junit.Ignore;
import org.junit.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Random;
import java.util.function.LongConsumer;

public class TestFlatMapAndConcatMap {

    /**
     * flatMap 测试
     * <p>
     * onSubscribe
     * request(256)
     * loop{
     * onNext(flux)
     * request(1)
     * }
     * <p>
     * 一直存在256个request等待onNext, 背压效果丢失
     */
    @Test
    @Ignore
    public void testFlatMap() {
        getFluxFlux().delayElements(Duration.ofMillis(200))
                .log()
                .flatMap(flux -> flux)
                .blockLast();
    }

    /**
     * concatMap 测试
     * <p>
     * onSubscribe
     * loop{
     * request(n)
     * onNext(flux) 1
     * onNext(flux) ...
     * onNext(flux) n
     * }
     * <p>
     * 保证背压效果
     */
    @Test
    @Ignore
    public void testConcatMap() {
        getFluxFlux().delayElements(Duration.ofMillis(200))
                .log()
                .concatMap(flux -> flux)
                .blockLast();
    }

    private static Flux<Flux<Integer>> getFluxFlux() {
        return Flux.create(fluxFluxSink -> {
            fluxFluxSink.onRequest(new LongConsumer() {
                @Override
                public void accept(long value) {
                    for (int i = 0; i < value; i++) {
                        fluxFluxSink.next(getFlux());
                    }
                }
            });
        });
    }

    public static Flux<Integer> getFlux() {
        return Flux.range(0, new Random().nextInt(100));
    }

}
