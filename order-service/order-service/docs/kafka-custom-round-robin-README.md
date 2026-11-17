# README --- Custom Kafka Round Robin Publishing Flow

## 1. Purpose

This document explains the **custom Round Robin partition-selection
flow** used in the Spring Boot and Apache Kafka example.

The goal is to understand: - Every application file involved in the
Round Robin flow. - Why Kafka metadata is discovered dynamically. - How
the partition count is stored and refreshed. - How the counter selects a
partition. - How the producer explicitly sends a record to that
partition. - How the consumer receives the record. - What changes when
the topic gains partitions.

This document is explanation-focused and intentionally does not
reproduce Java implementation code.

------------------------------------------------------------------------

## 2. Flow overview

The custom Round Robin flow publishes events to `stock_topics`. Before
publishing, the application discovers the topic's partition count. The
producer then asks a Partition Manager for the next partition and
explicitly supplies that partition to Kafka.

``` text
APPLICATION STARTUP
       |
       v
KafkaPartitionManager
       |
       v
AdminClient requests topic metadata
       |
       v
Kafka describes stock_topics
       |
       v
Discover and store partition count
       |
       v
Ready for publishing


MESSAGE PUBLISHING
       |
       v
Business operation creates OrderEvent
       |
       v
OrderProducerUsingRoundRobin
       |
       v
Ask KafkaPartitionManager for next partition
       |
       v
Atomic counter selects partition
       |
       v
KafkaTemplate sends to explicit partition
       |
       v
stock_topics partition
       |
       v
Kafka broker stores record
       |
       v
StockConsumer receives event
```

### Main idea

Normal publishing delegates partition selection to Kafka's producer
partitioner.

Custom Round Robin performs partition selection in the application
first, then tells Kafka which partition to use.

------------------------------------------------------------------------

## 3. Topic and environment

  Setting                         Value
  ------------------------------- ----------------------------------
  Topic                           `stock_topics`
  Broker                          `localhost:9092`
  Initial partition count         3
  Replication factor              1
  Consumer group in the example   `stock-group`
  Metadata refresh interval       60,000 milliseconds (60 seconds)
  Application HTTP port           8080

The topic is created manually using Kafka CLI commands in the source
notes. It is not created through a Spring `NewTopic` bean in this
design.

The topic must exist before the initial metadata lookup succeeds.

------------------------------------------------------------------------

## 4. Explanation of every participating application file

### 4.1 `application.yml`

**Purpose:** Supplies Kafka connection details, producer and consumer
serialization settings, topic names, and the metadata refresh interval.

Important settings:

  -----------------------------------------------------------------------------------------------------
  Setting                                                           Meaning
  ----------------------------------------------------------------- -----------------------------------
  `spring.kafka.bootstrap-servers`                                  Kafka broker address,
                                                                    `localhost:9092`.

  `spring.kafka.producer.key-serializer`                            Serializes the record key as a
                                                                    String.

  `spring.kafka.producer.value-serializer`                          Serializes the event value as JSON.

  `spring.kafka.consumer.group-id`                                  Consumer group ID, `stock-group`.

  `spring.kafka.consumer.auto-offset-reset`                         Uses the earliest available record
                                                                    if no valid committed offset
                                                                    exists.

  `spring.kafka.consumer.enable-auto-commit`                        Enables automatic offset commits in
                                                                    the example.

  `spring.kafka.consumer.key-deserializer`                          Deserializes the record key as a
                                                                    String.

  `spring.kafka.consumer.value-deserializer`                        Deserializes the JSON value into
                                                                    the event object.

  `spring.kafka.consumer.properties.spring.json.trusted.packages`   Configures trusted packages for
                                                                    JSON deserialization. The example
                                                                    uses `*`; review this choice for
                                                                    production.

  `spring.kafka.topic.second-name`                                  The Round Robin topic name,
                                                                    `stock_topics`.

  `kafka.topic.partition-refresh-ms`                                Refresh interval for topic
                                                                    metadata, `60000` milliseconds.
  -----------------------------------------------------------------------------------------------------

**Why this file matters:** The topic name and refresh interval can be
changed through configuration rather than being embedded in the
partition-selection logic.

### 4.2 `KafkaAdminConfig.java`

**Purpose:** Creates the Kafka `AdminClient` bean used to query Kafka
metadata.

Responsibilities: - Reads the Kafka broker address from configuration. -
Builds the AdminClient configuration. - Creates an AdminClient connected
to the configured Kafka cluster. - Configures the bean to close when the
Spring application context shuts down.

**Why this file is separate:** AdminClient is an infrastructure
dependency. Keeping its creation in a configuration class avoids mixing
client setup with partition-management responsibilities.

**What it does not do:** - It does not publish business events. - It
does not choose the next Round Robin partition. - It does not consume
records. - It does not itself run the scheduled refresh.

### 4.3 `KafkaPartitionManager.java`

`<badge color="success">`{=html}Core Round Robin
component`</badge>`{=html}

**Purpose:** Discovers, stores, and refreshes the partition count for
`stock_topics`, and provides the next partition number to the producer.

This class has several responsibilities.

#### A. Startup initialization

After the Spring Boot application is ready, the Partition Manager
performs its initial metadata lookup.

Why: - The producer needs a valid partition count before selecting a
partition. - The topic is expected to have been created already. -
Startup discovery avoids hardcoding the initial count in the producer.

#### B. Fetch topic metadata

The Partition Manager asks AdminClient to describe `stock_topics`.

Kafka returns a topic description containing its partition information.
The Partition Manager counts the partitions and stores that count in
memory.

For example, if Kafka reports three partitions, the in-memory count
becomes 3.

#### C. Store the current partition count

The current partition count is held in a thread-safe integer value.

The producer reads this value when it asks for the next partition. It
does not need to call Kafka's AdminClient for every message.

This is important: metadata lookup is performed during initialization
and periodic refresh, not as part of every publish request.

#### D. Select the next partition

The class maintains a separate atomic counter.

For a partition count of 3, the selection sequence is:

    Counter position   Selected partition
  ------------------ --------------------
                   0                    0
                   1                    1
                   2                    2
                   3                    0
                   4                    1
                   5                    2

The counter advances after each selection. The selection calculation
wraps the counter value by the current partition count.

#### E. Refresh metadata periodically

A scheduled method refreshes the partition count every 60 seconds, using
the configured refresh interval.

If Kafka reports a new count, the in-memory count is updated. If refresh
fails, the example logs the failure and keeps the last known count so a
temporary metadata failure does not immediately replace it with an
invalid value.

#### F. Validate readiness

If the partition count has not been initialized or is not positive, the
manager cannot safely return a partition number. The design treats this
as an error instead of guessing a partition.

### 4.4 `OrderProducerUsingRoundRobin.java`

**Purpose:** Publishes an event to `stock_topics` after asking the
Partition Manager for the next partition.

Responsibilities: - Receives the `OrderEvent`. - Uses the configured
Round Robin topic name. - Requests the next partition from
`KafkaPartitionManager`. - Sends the event through `KafkaTemplate`,
explicitly providing the topic, partition, key, and value. - Logs which
partition was selected. - Observes the asynchronous send result and logs
success metadata or a send failure.

The four send details are:

  Detail      Meaning
  ----------- ----------------------------------------------
  Topic       `stock_topics`
  Partition   The number selected by the Partition Manager
  Key         The order ID in the example
  Value       The `OrderEvent` object

**Important:** When the producer explicitly supplies a partition, Kafka
writes the record to that partition. The key is still stored in the
record, but it does not override the explicitly selected partition.

**What this file does not do:** - It does not query AdminClient
directly. - It does not calculate the partition count. - It does not own
the metadata refresh schedule. - It does not consume messages.

### 4.5 `StockConsumer.java`

**Purpose:** Consumes records from the configured topics.

The source example subscribes to both: - `order_topics` - `stock_topics`

For the Round Robin flow, it receives records from `stock_topics`.

Responsibilities: - Registers a Kafka listener for both topics. - Uses
the configured consumer group ID. - Receives an `OrderEvent`. - Reads
the actual received topic from the record header. - Logs the topic and
event.

The consumer does not need to know how the producer selected the
partition. Kafka delivers records from the partitions assigned to the
consumer group.

### 4.6 `OrderServiceApplication.java`

**Purpose:** Starts the Spring Boot application and enables scheduled
task execution.

The scheduling support is important for this flow because the Partition
Manager refreshes topic metadata periodically.

Without scheduling being enabled, the scheduled refresh method would not
run as intended. The initial startup lookup is a separate operation and
is triggered when the application is ready.

### 4.7 `OrderEvent` and its order data

**Purpose:** Represents the event payload published to Kafka.

The Round Robin producer uses the order ID as the Kafka record key in
the shown flow and sends the `OrderEvent` as the value.

The source notes refer to the event and its nested order data but do not
provide the full class definition. This document therefore does not
assume additional fields.

### 4.8 Controller or business caller

The source notes describe triggering the producer through a REST API,
but do not provide the complete controller implementation or endpoint
details.

Conceptually, the controller or business service initiates the
publishing operation and invokes `OrderProducerUsingRoundRobin`. Its
exact class name and API contract are not established by the supplied
notes.

------------------------------------------------------------------------

## 5. Understanding the Round Robin calculation

Assume the topic has three partitions: 0, 1, and 2.

The counter advances as messages request a partition. The selection
wraps around when the counter reaches the partition count.

  -----------------------------------------------------------------------
    Message request     Counter value   Partition count          Selected
                                 used                           partition
  ----------------- ----------------- ----------------- -----------------
                  1                 0                 3                 0

                  2                 1                 3                 1

                  3                 2                 3                 2

                  4                 3                 3                 0

                  5                 4                 3                 1

                  6                 5                 3                 2
  -----------------------------------------------------------------------

The same pattern continues until the partition count changes.

### Why `AtomicInteger`?

Multiple requests can publish concurrently. An atomic counter ensures
that counter increments happen safely within one application instance.

A normal shared integer could be subject to race conditions when
multiple threads read and update it at the same time.

### What does thread-safe mean here?

It means the counter's individual atomic operations are safe across
threads in this one running application instance.

It does **not** mean: - All Spring Boot instances share one counter. -
Messages are processed in one global order. - Kafka send completions
occur in the same order as partition selections.

------------------------------------------------------------------------

## 6. Dynamic partition discovery and refresh

### 6.1 Initial discovery

At application startup:

1.  The application becomes ready.
2.  The Partition Manager requests the description of `stock_topics`.
3.  Kafka returns its partition information.
4.  The manager counts the partitions.
5.  The manager stores the count in memory.
6.  The Round Robin producer can select a valid partition.

### 6.2 Periodic refresh

Every 60 seconds in the example:

1.  The scheduled task runs.
2.  AdminClient requests the latest topic description.
3.  The manager counts the current partitions.
4.  The stored count is updated.
5.  Future publishing requests use the refreshed count.

If the refresh fails, the example retains the previous count and logs
the error.

### 6.3 Example: increase from three to five partitions

Initially:

    Partition Available
  ----------- -----------
            0 Yes
            1 Yes
            2 Yes

An administrator increases the topic to five partitions:

    Partition Available
  ----------- -----------
            0 Yes
            1 Yes
            2 Yes
            3 Yes
            4 Yes

After the next successful metadata refresh, the stored count becomes 5.
New partition selections use the updated count and can select partitions
0 through 4.

### 6.4 Does the sequence restart at partition 0?

Not necessarily. The example does not reset the atomic counter when the
partition count changes. The next partition depends on the counter's
current value and the new partition count.

### 6.5 Kafka limitation

Kafka supports increasing a topic's partition count, but not decreasing
it through the same partition-alter operation. Existing records are not
redistributed when partitions are added. New records can be written to
the expanded set.

------------------------------------------------------------------------

## 7. Testing the Round Robin flow

### Setup checks

-   [ ] Kafka is running at `localhost:9092`.
-   [ ] `stock_topics` exists.
-   [ ] The topic initially has three partitions.
-   [ ] The Spring Boot application starts.
-   [ ] The Partition Manager logs that it discovered three partitions.

### Publishing checks

-   [ ] Trigger the operation that calls `OrderProducerUsingRoundRobin`.
-   [ ] Publish six messages in a controlled, sequential test.
-   [ ] Check the producer logs for selected partitions.
-   [ ] Expect the sequence 0, 1, 2, 0, 1, 2 for this controlled test.
-   [ ] Confirm successful send metadata, including topic, partition,
    and offset.
-   [ ] Confirm that the consumer receives records from `stock_topics`.

A controlled sequential test is important. Concurrent requests can make
log completion order look different from the order in which the counter
selected partitions.

### Dynamic metadata checks

-   [ ] Increase `stock_topics` from three partitions to five.
-   [ ] Wait for the next successful scheduled refresh.
-   [ ] Confirm the log shows the previous count as 3 and current count
    as 5.
-   [ ] Publish additional events.
-   [ ] Confirm that future selections can use partitions 0 through 4.

------------------------------------------------------------------------

## 8. Important production considerations

### 8.1 The counter is per application instance

Each Spring Boot instance has its own `AtomicInteger`. If multiple
application instances run, each has an independent sequence. The
combined system does not have one globally coordinated Round Robin
sequence.

### 8.2 Round Robin may break per-entity ordering

Suppose events for the same order are sent to different partitions.
Kafka guarantees order within a partition, not across all partitions. If
all events for one order must remain ordered, key-based partitioning is
usually a better design.

### 8.3 Explicit partition selection takes precedence over key routing

The example includes the order ID as the record key, but it also
explicitly supplies the partition. The explicit partition determines the
destination. The key does not cause Kafka to choose another partition.

### 8.4 Refresh failures need monitoring

Retaining the last known count can help during a temporary metadata
outage, but it does not guarantee that the cached count is valid
forever. Log and monitor repeated refresh failures and define how the
application should behave if the topic changes while metadata cannot be
refreshed.

### 8.5 Startup dependency

The manually created topic must exist and Kafka must be reachable when
the initial metadata lookup runs. Otherwise, the manager cannot
initialize a valid partition count.

### 8.6 More partitions do not automatically mean faster processing

Partitions can increase the potential parallelism of a consumer group,
but actual throughput depends on consumer instances, processing time,
broker capacity, and downstream services.

### 8.7 Offset and processing reliability

The example enables automatic offset commits. For business-critical
processing, ensure the offset strategy matches the required processing
guarantees. A committed offset should not be mistaken for proof that
every business side effect completed successfully.

### 8.8 Single-broker replication

The example uses replication factor 1 because it uses one broker.
Production availability requires a cluster and replication settings
appropriate to the failure-tolerance requirements.

------------------------------------------------------------------------

## 9. File responsibility summary

  -------------------------------------------------------------------------
  File                                  Main responsibility
  ------------------------------------- -----------------------------------
  `application.yml`                     Broker, serializer/deserializer,
                                        topic, consumer group, and refresh
                                        settings

  `KafkaAdminConfig.java`               Creates the AdminClient bean

  `KafkaPartitionManager.java`          Discovers and stores partition
                                        count, refreshes metadata, and
                                        selects the next partition

  `OrderProducerUsingRoundRobin.java`   Gets the selected partition and
                                        publishes the event to it

  `StockConsumer.java`                  Listens to both topics and receives
                                        events

  `OrderServiceApplication.java`        Starts Spring Boot and enables
                                        scheduling

  `OrderEvent` and order data           Defines the event payload sent
                                        through Kafka

  Controller/business caller            Initiates the publishing operation;
                                        exact source details were not
                                        included in the notes
  -------------------------------------------------------------------------

------------------------------------------------------------------------

## 10. Quick revision

**Custom Round Robin means:**

1.  Kafka topic partitions are discovered through AdminClient.
2.  The Partition Manager stores the current partition count.
3.  A scheduled task refreshes that count periodically.
4.  An atomic counter chooses the next partition.
5.  The Round Robin producer explicitly sends the event to that
    partition.
6.  The consumer receives the event from its assigned partition.

**Remember:** AdminClient discovers metadata. The Partition Manager
manages the count and selection. The producer sends the event. The
consumer receives it.

The responsibilities are separate; keeping them separate makes the
design easier to understand, test, and maintain.
