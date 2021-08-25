
## Quick Start with Greyhound Java API with Docker based app
This guide will walk you through the process of launching a simple greyhound application.
The app will be based on Spring Boot and Greyhound Java API.
We will create a Docker image from our app and run it together with a live Kafka, using docker compose.

![quick-start demo](https://github.com/wix/greyhound/blob/master/getting-started/assets/quick-start.gif)

### Prerequisites
 - Your favorite text editor or IDE
 - [JDK 1.8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or later
 - [Gradle 4+](http://www.gradle.org/downloads) or [Maven 3.2+](https://maven.apache.org/download.cgi)
 - [Docker Compose](https://docs.docker.com/compose/install/)

### Steps to launch the app
1. Start Docker
1. Build greyhound app and the Docker image:

		cd /getting-started/greyhound-app

	  **Gradle**

	    ./gradlew bootBuildImage --imageName=wixpress/greyhound-app

	  **Maven**

	    ./mvnw spring-boot:build-image -Dspring-boot.build-image.imageName=wixpress/greyhound-app

1. Start kafka, zookeeper and greyhound-app using docker-compose:

		cd ..
		docker-compose up -d

1. View greyhound-app logs in console:

		docker-compose logs -f greyhound_app
		docker-compose logs -f kafka

1. Check greyhound-app is running:

		Navigate in your browser to http://localhost:8080
		If you see `Hello Greyhound Application` we're good to go. If not, look for the cause in the logs or add debug logs to identify what's wrong (uncomment `debug: true` in the file `application.yml`)

1. After you did the next section and/or you want to change some code and start again, you'll need to shut down the greyhound-app, kafka and zookeeper. To do so run this command:

		docker-compose down
		
1. Once you change the app code:

        docker-compose down 
        repeat stages 2-4
        

### Produce and Consume messages

#### MaxParallelism Example
This example consists of 1 producer and 2 consumer groups. 

One consumer has no parallel message handling. The other consumer has the maximum possible parallelism (for a topic with 8 partitions).

Both message handlers have artificial delay of 1 ms.

1. Use curl `curl -X GET 'localhost:8080/produce?numOfMessages=1000'`  
   or navigate in your browser to http://localhost:8080/produce?numOfMessages=1000
1. Check the greyhound-app logs, wait for the last message to be consumed, and the summary to appear:
    ```
     greyhound_app_1  | Consumer with MaxParallelism = 8 - All messages consumed in 848 millis at Wed Sep 23 08:35:30 GMT 2020
     greyhound_app_1  | ----------------------------------------------------------------------
     greyhound_app_1  | Consumer with MaxParallelism = 1 - All messages consumed in 2151 millis at Wed Sep 23 08:35:32 GMT 2020
    ```
1. **Notice the difference**, it takes less than half the time to consume all the messages using greyhound feature of __max parallelism__!

#### Batch Consumer Example
This example consists of 1 producer and 2 consumer groups.

One consumer get messages one-by-one. The other consumer gets batches of messages.

Both message handlers have artificial delay of 1 ms.

1. Use curl `curl -X GET 'localhost:8080/produce-batch?numOfMessages=2000'`  
   or navigate in your browser to http://localhost:8080/produce-batch?numOfMessages=2000
1. Check the greyhound-app logs, wait for the last message to be consumed, and the summary to appear:
    ```
     greyhound_app_1  | ----------------------------------------------------------------------
     greyhound_app_1  | Batch consumer processed 2000 messages in 22 operations, consumed in 359 millis at Wed Aug 25 14:54:42 UTC 2021
     greyhound_app_1  | Operations and sizes per partition:
     greyhound_app_1  | Partition 0 - 3 operation(s) with size(s): [129, 2, 119]
     greyhound_app_1  | Partition 1 - 4 operation(s) with size(s): [78, 41, 128, 3]
     greyhound_app_1  | Partition 2 - 3 operation(s) with size(s): [125, 123, 2]
     greyhound_app_1  | Partition 3 - 2 operation(s) with size(s): [122, 128]
     greyhound_app_1  | Partition 4 - 2 operation(s) with size(s): [126, 124]
     greyhound_app_1  | Partition 5 - 4 operation(s) with size(s): [46, 79, 119, 6]
     greyhound_app_1  | Partition 6 - 2 operation(s) with size(s): [127, 123]
     greyhound_app_1  | Partition 7 - 2 operation(s) with size(s): [127, 123]
     greyhound_app_1  | ----------------------------------------------------------------------
     greyhound_app_1  | Regular consumer processed 2000 messages in 2000 operations, consumed in 1402 millis at Wed Aug 25 14:54:42 UTC 2021
     greyhound_app_1  | Operations per partition:
     greyhound_app_1  | Partition 0 - 250 operation(s)
     greyhound_app_1  | Partition 1 - 250 operation(s)
     greyhound_app_1  | Partition 2 - 250 operation(s)
     greyhound_app_1  | Partition 3 - 250 operation(s)
     greyhound_app_1  | Partition 4 - 250 operation(s)
     greyhound_app_1  | Partition 5 - 250 operation(s)
     greyhound_app_1  | Partition 6 - 250 operation(s)
     greyhound_app_1  | Partition 7 - 250 operation(s)
    ```
1. **Notice the difference**, the greyhound __batch consumer__ consumed all the messages in 100x fewer operations!

### Credits
- The spring boot application is based on: [https://spring.io/guides/gs/spring-boot-docker/](https://spring.io/guides/gs/spring-boot-docker/)
- Docker compose is based on: [https://github.com/confluentinc/kafka-images/blob/master/examples/kafka-single-node/docker-compose.yml](https://github.com/confluentinc/kafka-images/blob/master/examples/kafka-single-node/docker-compose.yml)
