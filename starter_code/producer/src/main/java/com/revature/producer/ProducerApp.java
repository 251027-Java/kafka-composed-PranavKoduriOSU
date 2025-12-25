package com.revature.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

@SpringBootApplication
@RestController
@RequestMapping("/api/messages")
public class ProducerApp {

    private static final Map<String, String> TOPICS = Map.of(
        "message", "messages",
        "order", "orders",
        "notification", "notifications"
    );

    // TODO: Inject KafkaTemplate
     private final KafkaTemplate<String, String> kafkaTemplate;

    // TODO: Add constructor injection
     public ProducerApp(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
     }

    public static void main(String[] args) {
        SpringApplication.run(ProducerApp.class, args);
    }

    @PostMapping
    public Map<String, String> sendMessage(@RequestBody Map<String, String> payload) {
        String type = null;
        String message = null;

        for (String messageType : TOPICS.keySet()){
            message = payload.get(messageType);
            if (message != null) {
                type = messageType;
                break;
            }
        }

        if (type == null || message.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Non-empty message with a valid topic is required.");
        }


        // TODO: Send message to Kafka
         kafkaTemplate.send(TOPICS.get(type), message);

        System.out.println("Sending message: " + message);

        return Map.of(
                "status", "sent",
                "topic", TOPICS.get(type),
                String.format("\"%s\"", type), message);
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "UP", "service", "producer");
    }
}
