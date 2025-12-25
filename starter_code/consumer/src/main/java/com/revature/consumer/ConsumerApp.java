package com.revature.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootApplication
@RestController
@RequestMapping("/api/messages")
public class ConsumerApp {

    private final Map<String, List<String>> receivedMessages = Map.of(
        "messages", new ArrayList<>(),
        "orders", new ArrayList<>(),
        "notifications", new ArrayList<>()
    );

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApp.class, args);
    }

    // TODO: Add @KafkaListener annotation
    @KafkaListener(topics = "messages", groupId = "message-consumers")
    public void listenMessage(String message) {
        System.out.println("Received message: " + message);
        receivedMessages.get("messages").add(message);
    }

    @KafkaListener(topics = "orders", groupId = "order-consumers")
    public void listenOrder(String order) {
        System.out.println("Received order: " + order);
        receivedMessages.get("orders").add(order);
    }

    @KafkaListener(topics = "notifications", groupId = "notification-consumers")
    public void listenNotification(String notification) {
        System.out.println("Received notification: " + notification);
        receivedMessages.get("notifications").add(notification);
    }

    @GetMapping
    public Map<String, Object> getMessages(@RequestParam String type) {
        List<String> messages = receivedMessages.get(type);

        if (messages == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown message type: " + type);
        }

        return Map.of(
                "count", messages.size(),
                "messages", messages);
    }

    @DeleteMapping
    public Map<String, String> clearMessages() {
        for (List<String> l : receivedMessages.values()) l.clear();
        return Map.of("status", "cleared");
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "UP", "service", "consumer");
    }
}
