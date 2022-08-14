package ninja.wmatos.ecommerce;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {
    private final Consumer<String, T> consumer;
    private final ConsumerFunction<T> runner;
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

    private KafkaService(String groupId, ConsumerFunction<T> runner, Class<T> type, Map<String, String> overrideProperties) {
        this.runner = runner;
        this.consumer = new KafkaConsumer<>(getProperties(type, groupId, overrideProperties));
    }

    public KafkaService(String groupId, String topic, ConsumerFunction<T> runner, Class<T> type, Map<String, String> overrideProperties) {
        this(groupId, runner, type, overrideProperties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> runner, Class<T> type, Map<String, String> overrideProperties) {
        this(groupId, runner, type, overrideProperties);

        consumer.subscribe(topic);
    }

    public void run() {
        while (true) {
            ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.printf("%d registers found\n", records.count());

                for (var record : records) {
                    runner.consume(record);
                }
            }
        }
    }

    private Properties getProperties(Class<T> type, String groupId, Map<String, String> overrideProperties) {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, groupId + '-' + UUID.randomUUID());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.putAll(overrideProperties);

        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
