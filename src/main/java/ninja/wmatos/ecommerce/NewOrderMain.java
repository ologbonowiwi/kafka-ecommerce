package ninja.wmatos.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;

public class NewOrderMain {

    public static void main(String[] args) {
        try (KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>(); KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>()) {
            for (int i = 0; i < 100; i++) {
                String key = UUID.randomUUID().toString();

                String userId = UUID.randomUUID().toString();
                String orderId = UUID.randomUUID().toString();
                BigDecimal amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
                Order order = new Order(userId, orderId, amount);

                orderDispatcher.send("ECOMMERCE_NEW_ORDER", key, order);

                Email email = new Email("New order received!", "We are processing your order. Thank you for your preference!");
                emailDispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        }
    }
}
