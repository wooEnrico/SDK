import io.github.wooenrico.kafka.consumer.RateLimitExecutorConsumerProperties;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import io.github.wooenrico.kafka.consumer.ConsumerProperties;
import io.github.wooenrico.kafka.sender.SenderProperties;

@Testcontainers
public abstract class AbstractKafkaContainerTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    protected SenderProperties senderProperties;
    protected RateLimitExecutorConsumerProperties consumerProperties;

    @BeforeEach
    void setUp() {
        // Set up sender properties
        senderProperties = new SenderProperties();
        senderProperties.setEnabled(true);
        senderProperties.addProperties("bootstrap.servers", kafka.getBootstrapServers());
        senderProperties.addProperties("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        senderProperties.addProperties("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Set up consumer properties
        consumerProperties = new RateLimitExecutorConsumerProperties();
        consumerProperties.setEnabled(true);
        consumerProperties.addProperties("group.id", "test-consumer");
        consumerProperties.addProperties("bootstrap.servers", kafka.getBootstrapServers());
        consumerProperties.addProperties("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.addProperties("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.addProperties("auto.offset.reset", "earliest");
    }
}