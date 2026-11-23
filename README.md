# Fraud Detection Service

A Spring Boot and Kafka Streams demo that generates sample payment transactions, evaluates them for suspicious activity, and publishes fraud alerts to Kafka.

The service is event driven. It does not currently expose REST endpoints.

## What it does

When the application starts, it publishes sample account and merchant reference data. A scheduled simulator then publishes transactions to Kafka about once per second. Kafka Streams joins each transaction with account and merchant data, tracks activity in one-minute windows, and publishes matching alerts.

The current rules are:

| Rule | Condition |
| --- | --- |
| High value | Transaction amount is at least £5,000. |
| Blacklisted merchant | The transaction uses a merchant marked as blacklisted. |
| Velocity | More than three transactions for an account occur in a one-minute window. |
| Impossible travel | An account uses different country codes within the same one-minute window. |

The simulator produces normal transactions as well as high-value transactions, blacklisted-merchant transactions, rapid bursts, and two-leg travel scenarios.

## Kafka topics

| Topic | Purpose |
| --- | --- |
| `accounts` | Account reference data, keyed by account ID. |
| `merchants` | Merchant reference data, keyed by merchant ID. |
| `transactions-raw` | Simulated transactions consumed by the topology. |
| `fraud-alerts` | Fraud alerts emitted by the topology. |

The application declares these topics on startup. The account and merchant topics use log compaction.

## Requirements

- Java 17
- Docker Desktop with Docker Compose

## Run everything with Docker Compose

The Dockerfile copies the JAR from `build/libs`, so build the application before building the image:

```powershell
.\gradlew.bat bootJar
docker compose up --build
```

Compose starts Kafka and the application. Kafka must become healthy before the application starts. The application container connects to Kafka at `kafka:9092`.

To follow application logs in another terminal:

```powershell
docker compose logs -f fraud-detection-service
```

You should see reference data seeding messages and then `Sent transaction` messages. Stop the stack with `Ctrl+C` if running attached, or run `docker compose down` from another terminal.

## Run the application locally with Kafka in Docker

Start only Kafka:

```powershell
docker compose up -d kafka
```

The Compose broker advertises `localhost:29092` to clients running on your computer. Set the bootstrap server for the local process, then run the app:

```powershell
$env:SPRING_KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
.\gradlew.bat bootRun
```

The `kafka:9092` address is for containers on the Compose network; your local process cannot resolve the Docker hostname `kafka`.

For development, Spring Boot DevTools is included as a development-only dependency. After Gradle sync, run `bootRun` and use **Build → Build Project** (Ctrl+F9) in IntelliJ after editing. DevTools restarts the app when the changed classes are compiled; it does not compile source code itself.

## Inspect Kafka

List topics:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Read transactions from the beginning:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions-raw --from-beginning
```

Read fraud alerts:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fraud-alerts --from-beginning
```

Stop a consumer with `Ctrl+C`.

## Tests

Run the test suite:

```powershell
.\gradlew.bat test
```

The topology tests use Kafka's `TopologyTestDriver`, so they do not need a running Kafka broker. The Spring context test disables the simulator and Kafka Streams auto startup so it can check bean wiring without external Kafka services.

## Technology

- Java 17 and Spring Boot
- Apache Kafka and Kafka Streams
- Gradle
- Docker Compose
