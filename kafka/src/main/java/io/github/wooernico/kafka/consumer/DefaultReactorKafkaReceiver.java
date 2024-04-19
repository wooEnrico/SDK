package io.github.wooernico.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverPartition;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;

public class DefaultReactorKafkaReceiver extends ReactorKafkaReceiver<String, String> implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(DefaultReactorKafkaReceiver.class);

    public DefaultReactorKafkaReceiver(String name, ConsumerProperties consumerProperties, Function<ConsumerRecord<String, String>, Mono<Void>> consumer) {
        super(name, consumerProperties, consumer, new StringDeserializer(), new StringDeserializer());
    }

    public DefaultReactorKafkaReceiver(String name, ConsumerProperties consumerProperties, Function<ConsumerRecord<String, String>, Mono<Void>> consumer, Consumer<Collection<ReceiverPartition>> onAssign, Consumer<Collection<ReceiverPartition>> onRevoke) {
        super(name, consumerProperties, new StringDeserializer(), new StringDeserializer(), consumer, onAssign, onRevoke);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("default reactor kafka receiver named [{}] init with {}", this.name, this.consumerProperties);
    }

    @Override
    public void destroy() throws Exception {
        super.close();
    }
}
