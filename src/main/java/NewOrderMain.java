import java.util.UUID;

public class NewOrderMain {

    public static void main(String[] args) {
        KafkaDispatcher dispatcher = new KafkaDispatcher();

        for (int i = 0; i < 100; i++) {
            String key = UUID.randomUUID().toString();

            String newOrderValue = "123214521,123125412321,21451253123"; // id_request, id_order, valor
            dispatcher.send("ECOMMERCE_NEW_ORDER", key, newOrderValue);

            String emailValue = "Thank you for your order! We are processing your order!";
            dispatcher.send("ECOMMERCE_SEND_EMAIL", key, emailValue);
        }
    }
}
