import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Producer<Object, String> producer = new KafkaProducer<>(properties());

        String value = "123214521,123125412321,21451253123"; // id_request, id_order, valor
        ProducerRecord<Object, String> record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", null, value);

        producer.send(record, (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }

            System.out.printf("Message sent successfully %s | partition %d | offset | %d%n | time %d%n", data.topic(), data.partition(), data.offset(), data.timestamp());
        }).get();
    }

    private static @NotNull Properties properties() {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
