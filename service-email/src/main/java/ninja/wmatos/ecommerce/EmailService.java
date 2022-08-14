package ninja.wmatos.ecommerce;

import org.apache.kafka.clients.consumer.*;

import java.util.Map;

public class EmailService {
    public static void main(String[] args) {
        var emailService = new EmailService();

        try (KafkaService<Email> service = new KafkaService<>(EmailService.class.getName(), "ECOMMERCE_SEND_EMAIL", emailService::runner, Email.class, Map.of())) {
            service.run();
        }
    }

    private void runner(ConsumerRecord<String, Email> record) {
        System.out.println("-------------------------------");
        System.out.println("Sending email");
        System.out.printf("Key: %s\n", record.key());
        System.out.printf("Value: %s\n", record.value());
        System.out.printf("Partition: %d\n", record.partition());
        System.out.printf("Offset: %d%n\n", record.offset());
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
        System.out.println("Order processed");
    }
}
