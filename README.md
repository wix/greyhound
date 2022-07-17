# Greyhound

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.wix/greyhound-core_2.12/badge.svg?kill_cache=1)](https://maven-badges.herokuapp.com/maven-central/com.wix/greyhound-core_2.12) [![Github Actions](https://github.com/wix/greyhound/workflows/CI/badge.svg)](https://github.com/wix/greyhound/actions/)<br/>
High-level SDK for [Apache Kafka](https://kafka.apache.org/).<br/>
Available for Java, Scala and coming soon for JavaScript, Python, .Net

![Greyhound](docs/logo.png)

## Why Greyhound?

Kafka is shipped with a Java SDK which allows developers to interact with a Kafka cluster.
However, this SDK consists of somewhat low-level APIs which are difficult to use correctly.
Greyhound seeks to provide a higher-level interface to Kafka and to express richer
semantics such as parallel message handling or retry policies with ease.

You can read more about it on this blog post - “[Building a High-level SDK for Kafka: Greyhound Unleashed](https://www.wix.engineering/post/building-a-high-level-sdk-for-kafka-greyhound-unleashed)”

> 📝 Note:  
> The open source version of Greyhound is still in the initial rollout stage, so the APIs might not be fully stable yet.

## Available APIs

- Scala Futures
- Java
- ZIO based API
- More APIs coming soon...

## Main features

- **Declarative API**

  When you want to consume messages from Kafka using the consumer API
  provided in the Java SDK, you need to run an infinite _while loop_ which polls for new records,
  execute some custom code, and commit offsets.

  This might be fine for simple applications, however, it's hard
  to get all the subtleties right - especially when you want to ensure your processing guarantees
  (i.e., when do you commit), handle consumer rebalances gracefully, handle errors (Either originating from Kafka or from user's code), etc.

  This requires specific know-how and adds a lot of boilerplate when all you want to do is process messages from a topic.
  Greyhound tries to abstract away these complexities by providing a simple, declarative API, and
  to allow the developers to focus on their business logic instead of how to access Kafka correctly.

- **Parallel message handling**

  A single Kafka consumer is single-threaded, and if you want to
  achieve parallelism with your message handling (which might be crucial for high throughput
  topics) you need to manually manage your threads and/or deploy more consumer instances.

  Greyhound automatically handles parallelizing message handling for you with automatic throttling.
  Also, Greyhound uses a concurrency model based on fibers (or green-threads) which are much more
  lightweight than JVM threads, and makes async workloads extremely efficient.

- **Consumer retries**

  Error handling is tricky. Sometimes things fail without our control
  (database is temporarily down, API limit exceeded, network call timed-out, etc.) and the only
  thing we can do to recover is to retry the same operation after some back-off.

  However, we do not
  want to block our consumer until the back-off expires, nor do we want to retry the action in a
  different thread and risk losing the messages in case our process goes down. Greyhound provides
  a robust retry mechanism, which produces failed records to special retry topics where they will be
  handled later, allowing the main consumer to keep working while ensuring no messages will be lost.

- **Observability**

  Greyhound reports many useful metrics which are invaluable when trying to
  debug your system, or understand how it is operating.

## API Usage

### Basics

First let's review some basic messaging terminology:

- Kafka maintains feeds of messages in categories called topics.
- Processes that publish messages to a Kafka topic are called producers.
- Processes that subscribe to topics and process the feed of published messages are called consumers.
- Kafka is run as a cluster comprised of one or more servers, each of which is called a broker.

### Current APIs

To dive into the currently available APIs, follow the link to the relevant docs:

- [Scala Futures](docs/scala-futures-api.md)
- [Java](docs/java-api.md)
- [ZIO based API](docs/zio-based-api.md)
- [Non-JVM Languages](docs/non-jvm-languages.md)

All Greyhound modules can be found in the [Maven Central Repository](https://search.maven.org/search?q=greyhound).

See [examples](docs/build.md) of how to add greyhound modules to your build (Maven, Gradle, SBT, etc.)
