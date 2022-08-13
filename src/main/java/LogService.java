import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        Consumer<String, String> consumer = new KafkaConsumer<>(properties());

        consumer.subscribe(Pattern.compile("ECOMMERCE.*"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.printf("%d registers found\n", records.count());

                for (var record : records) {
                    System.out.println("-------------------------------");
                    System.out.println("Logging registers");
                    System.out.printf("Topic: %s\n", record.topic());
                    System.out.printf("Key: %s\n", record.key());
                    System.out.printf("Value: %s\n", record.value());
                    System.out.printf("Partition: %d\n", record.partition());
                    System.out.printf("Offset: %d%n\n", record.offset());
                }
            }
        }
    }

    private static @NotNull Properties properties() {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, LogService.class.getName() + "-" + UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        return properties;
    }
}
