import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher implements Closeable {
    private final KafkaProducer<String, String> producer = new KafkaProducer<>(properties());
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

    public void send(String topic, String key, String value) {
        ProducerRecord<String, String> message = new ProducerRecord<>(topic, key, value);

        try {
            this.producer.send(message, getCallback()).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
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

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

    @Override
    public void close() {
        producer.close();
    }
}