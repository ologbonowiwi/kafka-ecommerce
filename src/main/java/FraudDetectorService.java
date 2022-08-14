import org.apache.kafka.clients.consumer.*;

public class FraudDetectorService {
    public static void main(String[] args) {
        FraudDetectorService fraudDetector = new FraudDetectorService();
        try (KafkaService service = new KafkaService(FraudDetectorService.class.getName(), "ECOMMERCE_NEW_ORDER", fraudDetector::runner)) {
            service.run();
        }
    }

    private void runner(ConsumerRecord<String, String> record) {
        System.out.println("-------------------------------");
        System.out.println("Checking for fraud on new order");
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
