package ninja.wmatos.ecommerce;

public class Email {
    private final String subject, body;

    public Email(String subject, String body) {
        this.body = body;
        this.subject = subject;
    }
}
