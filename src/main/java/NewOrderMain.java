import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Producer<Object, String> producer = new KafkaProducer<>(properties());

        String newOrderValue = "123214521,123125412321,21451253123"; // id_request, id_order, valor
        ProducerRecord<Object, String> newOrderRecord = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", null, newOrderValue);

        producer.send(newOrderRecord, getCallback()).get();

        String emailValue = "Thank you for your order! We are processing your order!";
        ProducerRecord<Object, String> emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", null, emailValue);

        producer.send(emailRecord, getCallback()).get();
    }

    @NotNull
    private static Callback getCallback() {
        return (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }

            System.out.printf("Message sent successfully %s | partition %d | offset | %d%n | time %d%n", data.topic(), data.partition(), data.offset(), data.timestamp());
        };
    }

    private static @NotNull Properties properties() {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
