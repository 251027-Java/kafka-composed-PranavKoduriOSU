[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/QMu7oXfh)
# Lab: Kafka with Docker Compose

## Goal
Set up a Kafka messaging system using Docker Compose and create a Spring Boot application that produces and consumes messages.

## Learning Objectives
- Deploy Kafka and Zookeeper using Docker Compose
- Configure Spring Boot to connect to containerized Kafka
- Create Kafka producers and consumers
- Test message flow between services

## Pre-requisites
- Docker and Docker Compose installed
- Java 17+ and Maven installed
- Understanding of Kafka basics (topics, producers, consumers)

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Docker Compose                         │
│                                                          │
│  ┌──────────────┐    ┌──────────────┐                  │
│  │  Zookeeper   │◄───│    Kafka     │                  │
│  │    :2181     │    │    :9092     │                  │
│  └──────────────┘    └──────┬───────┘                  │
│                             │                           │
│              ┌──────────────┴──────────────┐           │
│              │                              │           │
│       ┌──────▼──────┐              ┌───────▼──────┐    │
│       │  Producer   │              │   Consumer   │    │
│       │  Service    │──[topic]────►│   Service    │    │
│       │   :8080     │              │    :8081     │    │
│       └─────────────┘              └──────────────┘    │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

## Tasks

### Task 1: Create Docker Compose for Kafka
Create a `docker-compose.yml` that includes:
- Zookeeper (port 2181)
- Kafka (port 9092)
- Network for services to communicate

**Starter Template Provided**: See `starter_code/docker-compose.yml`

Fill in the TODO sections:
- Kafka environment variables
- Volume mappings
- Health checks

### Task 2: Start Kafka Infrastructure
```bash
docker-compose up -d zookeeper kafka
docker-compose logs kafka
```

Verify Kafka is running:
```bash
# Create a test topic
docker exec kafka kafka-topics --create --topic test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# List topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Task 3: Complete the Producer Service
Complete the `ProducerService.java`:
1. Configure `KafkaTemplate<String, String>`
2. Create a REST endpoint to send messages
3. Add proper error handling

**Endpoint**: `POST /api/messages`
**Body**: `{ "message": "Hello Kafka!" }`

### Task 4: Complete the Consumer Service
Complete the `ConsumerService.java`:
1. Create a `@KafkaListener` method
2. Log received messages
3. Store messages in a list for retrieval

**Endpoint**: `GET /api/messages` - Returns all received messages

### Task 5: Add Services to Docker Compose
Add the producer and consumer services to `docker-compose.yml`:
- Build from Dockerfile
- Connect to Kafka network
- Set environment variables for Kafka bootstrap servers

### Task 6: Test the Complete System
```bash
# Build and start everything
docker-compose up --build

# Send a message
curl -X POST http://localhost:8080/api/messages \
  -H "Content-Type: application/json" \
  -d '{"message": "Hello from Docker!"}'

# Check consumer received it
curl http://localhost:8081/api/messages
```

### Task 7: Multiple Topics (Bonus)
Create additional topics for different message types:
- `orders` - Order events
- `notifications` - Notification events

## Deliverables
1. Completed `docker-compose.yml`
2. Working Producer Service
3. Working Consumer Service
4. Screenshot of successful message flow
5. Docker Compose logs showing Kafka activity

## Starter Code
- `docker-compose.yml` - Partial Kafka setup
- `producer/` - Producer service skeleton
- `consumer/` - Consumer service skeleton

## Hints
- Kafka inside Docker uses `KAFKA_ADVERTISED_LISTENERS` for external access
- Spring Boot uses `spring.kafka.bootstrap-servers` property
- Use `depends_on` with health checks to ensure proper startup order
