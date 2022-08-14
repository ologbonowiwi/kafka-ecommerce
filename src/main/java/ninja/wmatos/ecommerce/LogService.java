package ninja.wmatos.ecommerce;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        LogService logService = new LogService();
        try (KafkaService<String> service = new KafkaService<>(LogService.class.getName(),
                Pattern.compile("ECOMMERCE.*"),
                logService::runner,
                String.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            service.run();
        }
    }

    private void runner(ConsumerRecord<String, String> record) {
        System.out.println("-------------------------------");
        System.out.println("Logging registers");
        System.out.printf("Topic: %s\n", record.topic());
        System.out.printf("Key: %s\n", record.key());
        System.out.printf("Value: %s\n", record.value());
        System.out.printf("Partition: %d\n", record.partition());
        System.out.printf("Offset: %d%n\n", record.offset());
    }
}
