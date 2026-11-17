# README --- Normal Kafka Publish--Subscribe Flow

## 1. Purpose

This document explains the **normal Kafka Publish--Subscribe flow** used
in the Spring Boot example.

The goal is to understand: - Which application files participate in this
flow. - What responsibility each file has. - How a message moves from
the producer to Kafka and then to the consumer. - What the important
Kafka and Spring Kafka settings mean. - How to test and troubleshoot the
flow.

This document is intentionally explanation-focused. It does not
reproduce Java implementation code.

------------------------------------------------------------------------

## 2. Flow overview

The normal flow publishes an order event to the Kafka topic
`order_topics`. The application does not explicitly choose a partition.
The Kafka producer's partitioner selects the destination partition.

``` text
Business operation
       |
       v
Create OrderEvent
       |
       v
OrderProducer
       |
       v
KafkaTemplate
       |
       v
order_topics
       |
       v
Kafka producer partitioner selects partition
       |
       v
Kafka broker stores the record
       |
       v
StockConsumer
       |
       v
Process OrderEvent
```

### Step-by-step

1.  A business operation, such as creating an order, produces an
    `OrderEvent`.
2.  The application calls the normal producer.
3.  The producer builds a Spring message containing the event payload
    and the topic header.
4.  `KafkaTemplate` sends the message to Kafka.
5.  No partition number is supplied by the application.
6.  Kafka's producer partitioner selects the destination partition.
7.  The broker appends the record to that partition.
8.  The consumer listener receives the event from its subscribed topic.
9.  The consumer processes the event.

------------------------------------------------------------------------

## 3. Topic used by this flow

  Setting                         Value
  ------------------------------- ------------------
  Topic                           `order_topics`
  Broker                          `localhost:9092`
  Initial partition count         1
  Replication factor              1
  Consumer group in the example   `stock-group`

The example uses one broker, so the replication factor is 1. This is a
local learning setup, not a highly available production configuration.

### Topic creation and verification

The original project notes create `order_topics` manually through the
Kafka command-line tools. The topic is expected to exist before messages
are published.

Useful checks: - List topics and confirm `order_topics` exists. -
Describe `order_topics` and confirm its partition count. - Confirm the
broker is reachable at `localhost:9092`.

------------------------------------------------------------------------

## 4. Explanation of every participating application file

The source notes show the following files and configuration. The
descriptions below explain their roles in the normal flow.

### 4.1 `application.yml`

**Purpose:** Central application and Kafka configuration.

Important settings:

  -----------------------------------------------------------------------------------------------------
  Setting                                                           Meaning
  ----------------------------------------------------------------- -----------------------------------
  `spring.application.name`                                         Identifies the Spring Boot
                                                                    application. The example name is
                                                                    `order-service`.

  `spring.kafka.bootstrap-servers`                                  Kafka broker address used by the
                                                                    application: `localhost:9092`.

  `spring.kafka.producer.key-serializer`                            Converts the message key into
                                                                    bytes. The example uses a String
                                                                    serializer.

  `spring.kafka.producer.value-serializer`                          Converts the event value into
                                                                    bytes. The example uses Spring
                                                                    Kafka's JSON serializer.

  `spring.kafka.consumer.group-id`                                  Identifies the consumer group. The
                                                                    example uses `stock-group`.

  `spring.kafka.consumer.auto-offset-reset`                         Controls where a group starts when
                                                                    it has no valid committed offset.
                                                                    `earliest` means the earliest
                                                                    available record.

  `spring.kafka.consumer.enable-auto-commit`                        Enables automatic offset commits in
                                                                    the example.

  `spring.kafka.consumer.key-deserializer`                          Converts key bytes back into a
                                                                    String.

  `spring.kafka.consumer.value-deserializer`                        Converts JSON bytes back into the
                                                                    event object.

  `spring.kafka.consumer.properties.spring.json.trusted.packages`   Configures which packages the JSON
                                                                    deserializer trusts. The source
                                                                    example uses `*`; broad trust
                                                                    should be reviewed carefully for
                                                                    production.

  `spring.kafka.topic.name`                                         The normal publishing topic:
                                                                    `order_topics`.

  `spring.kafka.topic.second-name`                                  The other topic used by the
                                                                    separate Round Robin flow:
                                                                    `stock_topics`. It is not the
                                                                    destination for this normal
                                                                    producer.

  `server.port`                                                     Spring Boot HTTP port, shown as
                                                                    `8080`.
  -----------------------------------------------------------------------------------------------------

**Why this file matters:** It keeps broker connection details,
serialization choices, consumer behavior, and topic names outside the
Java business logic.

### 4.2 `OrderProducer.java`

**Purpose:** Sends an `OrderEvent` to the normal topic, `order_topics`.

Responsibilities: - Receives an `OrderEvent` from the application. -
Uses Spring Kafka's `KafkaTemplate` to publish the event. - Reads the
topic name from application configuration. - Builds a Spring message
whose payload is the event. - Adds the Kafka topic name in the message
header. - Sends the message.

**Important behavior:** The producer does not provide a partition number
or a record key in the shown `MessageBuilder` flow. Therefore, the
application is not manually selecting a partition. The Kafka producer's
partitioner handles destination selection.

**What this file does not do:** - It does not discover the topic's
partition count. - It does not implement a Round Robin counter. - It
does not manage consumer offsets. - It does not process messages
received from Kafka.

### 4.3 `StockConsumer.java`

**Purpose:** Receives events from the topics configured in its listener
subscription.

The source example subscribes to both: - `order_topics` - `stock_topics`

For the normal flow, the relevant topic is `order_topics`.

Responsibilities: - Registers a Kafka listener. - Uses the configured
consumer group ID. - Receives an `OrderEvent`. - Reads the received
topic from the Kafka record header. - Logs the topic and event.

The received-topic header is useful because one listener can subscribe
to more than one topic. The consumer can identify which topic delivered
a particular event.

**Important:** A consumer listener is not the same thing as a consumer
group. The listener is application code; the group ID identifies the
group whose members share partition assignments and committed offsets.

### 4.4 `OrderServiceApplication.java`

**Purpose:** Starts the Spring Boot application.

The source notes show this class with: - The Spring Boot application
annotation. - The scheduling-enable annotation.

Scheduling is required by the separate Round Robin flow's periodic
metadata refresh. It is not needed by the normal producer itself, but
the same application entry point enables it for the combined example
application.

### 4.5 `OrderEvent` and its order data

The producer and consumer use an `OrderEvent` object as the message
payload.

Its role in this flow: - Carries the event data from the producer to
Kafka. - Is serialized to JSON by the producer configuration. - Is
deserialized back into an application object by the consumer
configuration.

The supplied notes refer to `OrderEvent` and its order data but do not
include the full source definition of that class. Therefore, this README
does not assume or document fields beyond those explicitly referenced in
the notes.

### 4.6 Controller or business caller

The notes mention triggering the producer through a REST API, but they
do not provide the controller's complete source file or endpoint
definition.

Conceptually, the controller or business service is the caller that
initiates publishing. It creates or receives the event and invokes
`OrderProducer`. Its exact class name, endpoint, request structure, and
validation rules are not established by the supplied notes.

------------------------------------------------------------------------

## 5. Important Kafka concepts in this flow

### Topic

A named stream where Kafka stores records. Here, it is `order_topics`.

### Partition

A topic is divided into partitions. Each partition is an ordered log.
The example topic initially has one partition.

### Producer partitioner

When the application does not specify a partition, the producer
partitioner chooses one. With a key, the default behavior generally uses
the key for partition selection. With a null key, modern Kafka producers
generally use sticky partitioning behavior rather than strict
message-by-message Round Robin.

Therefore, **normal publishing does not promise strict Round Robin
distribution**.

### Consumer group

The example uses `stock-group`. Consumers with the same group ID share
the work of consuming assigned topic partitions.

### Offset

An offset is a record's position inside one partition. Offsets are
tracked separately for each topic partition and consumer group.

### `auto-offset-reset: earliest`

This applies when the consumer group does not have a valid committed
offset for a partition. It does not force the consumer to replay from
the beginning every time the application restarts.

------------------------------------------------------------------------

## 6. Testing the normal flow

1.  Start the Kafka broker.
2.  Confirm that `order_topics` exists.
3.  Start the Spring Boot application.
4.  Trigger the business operation or REST endpoint that calls
    `OrderProducer`.
5.  Check the producer/application logs for send errors.
6.  Check the consumer logs for the received topic and event.
7.  Confirm that the received topic is `order_topics`.

A console consumer can also be used to inspect records in
`order_topics`. Use the broker address and topic name from this
document, and choose the appropriate deserialization approach for the
record format.

### Expected result

The producer publishes to `order_topics`, Kafka stores the record in a
partition, and the subscribed consumer group receives it.

Do not use a strict partition sequence such as 0, 1, 2, 0, 1, 2 as the
acceptance criterion for this normal flow. That sequence belongs to the
custom Round Robin design.

------------------------------------------------------------------------

## 7. Common troubleshooting

  -----------------------------------------------------------------------
  Symptom                             What to check
  ----------------------------------- -----------------------------------
  Application cannot connect to Kafka Broker status, `bootstrap-servers`,
                                      network access, and port `9092`.

  Topic not found                     Confirm `order_topics` was created
                                      in the connected Kafka cluster.

  Producer reports serialization      Confirm producer value serializer
  errors                              matches the event payload and
                                      consumer deserializer
                                      configuration.

  Consumer receives no records        Check topic name, group ID,
                                      listener startup, partition
                                      assignment, and committed offsets.

  Consumer starts at an unexpected    Check whether the group already has
  position                            a committed offset. `earliest`
                                      applies only when no valid
                                      committed offset exists.

  Consumer receives events from       The listener intentionally
  another topic                       subscribes to both configured
                                      topics; inspect the received-topic
                                      header.
  -----------------------------------------------------------------------

------------------------------------------------------------------------

## 8. Design notes and production considerations

1.  **Use a meaningful key when ordering by entity matters.** If all
    events for one order must be routed consistently, use a stable
    order-related key and let the configured partitioner route by key.
    The shown normal producer message does not explicitly set a key.
2.  **Ordering is per partition.** Kafka does not provide a single total
    order across all partitions.
3.  **Review automatic commits.** Automatic offset commits do not
    necessarily prove that business processing completed successfully
    before the offset was committed. Choose an acknowledgment strategy
    that matches processing requirements.
4.  **Review trusted packages.** Trusting every package is convenient in
    a local demonstration but should be assessed carefully in a
    production application.
5.  **Plan replication for production.** A replication factor of 1 on a
    single broker does not provide broker-failure tolerance.
6.  **Keep publishing and processing responsibilities separate.** The
    producer sends events; the consumer handles received events.

------------------------------------------------------------------------

## 9. Quick revision

**Normal Publish--Subscribe means:**

-   The application sends an event to `order_topics`.
-   `OrderProducer` uses `KafkaTemplate`.
-   No partition is explicitly supplied.
-   Kafka's producer partitioner selects the partition.
-   `StockConsumer` listens to the topic as part of `stock-group`.
-   The consumer receives and processes the event.

**Remember:** The application says *which topic* to publish to. In this
normal flow, it does not say *which partition*. Kafka's producer
partitioner makes that decision.
