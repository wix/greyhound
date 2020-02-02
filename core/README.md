# Greyhound 🐕

Opinionated SDK for [Apache Kafka](https://kafka.apache.org/)

## Why Greyhound?

Kafka is shipped with a Java SDK which allows developers to interact with a Kafka cluster.
However, this SDK consists of somewhat low-level APIs which are difficult to use correctly.
Greyhound seeks to provide a higher-level interface to Kafka and to express richer
semantics such as parallel message handling or retry policies with ease.

### Notable features Greyhound provides:

 * Declarative API - _TODO_
 * Parallel message handling - _TODO_
 * Context propagation - _TODO_
 * Retries - _TODO_
 * Safety - _TODO_
 * Observability - _TODO_

## Usage

### Basics

First let's review some basic messaging terminology:

 * Kafka maintains feeds of messages in categories called topics.
 * We'll call processes that publish messages to a Kafka topic producers.
 * We'll call processes that subscribe to topics and process the feed of published messages consumers.
 * Kafka is run as a cluster comprised of one or more servers each of which is called a broker.

### Producing messages

### Consuming messages
