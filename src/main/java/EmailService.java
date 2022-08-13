import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class EmailService {
    public static void main(String[] args) {
        Consumer<String, String> consumer = new KafkaConsumer<>(properties());

        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            if (!records.isEmpty()) {
                System.out.printf("%d registers found\n", records.count());

                for (var record : records) {
                    System.out.println("-------------------------------");
                    System.out.println("Sending email");
                    System.out.printf("Key: %s\n", record.key());
                    System.out.printf("Value: %s\n", record.value());
                    System.out.printf("Partition: %d\n", record.partition());
                    System.out.printf("Offset: %d%n\n", record.offset());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // ignoring
                        e.printStackTrace();
                    }
                    System.out.println("Order processed");
                }
            }
        }
    }

    private static @NotNull Properties properties() {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailService.class.getName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, LogService.class.getName() + "-" + UUID.randomUUID().toString());

        return properties;
    }
}
