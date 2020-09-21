
## Getting started with Greyhound and run it using Docker
This guide will walk you through the process of launching a simple greyhound application.
The app will be based on Spring Boot and Greyhound Java API.
We will create a Docker image from our app and run it together with a live Kafka, using docker compose.

### Prerequisites
  - A favorite text editor or IDE
 - [JDK 1.8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or later
 - [Gradle 4+](http://www.gradle.org/downloads) or [Maven 3.2+](https://maven.apache.org/download.cgi)
 - [Docker Compose](https://docs.docker.com/compose/install/)

### Steps to launch the app
1. Start Docker
2. Build greyhound app and the Docker image:

		cd /getting-started/greyhound-app

	  **Gradle**

	    ./gradlew bootBuildImage --imageName=wixpress/greyhound-app

	  **Maven**

	    ./mvnw spring-boot:build-image -Dspring-boot.build-image.imageName=wixpress/greyhound-app

3. Start kafka, zookeeper and greyhound-app using docker-compose:

		cd ..
		docker-compose up -d

4. View greyhound-app logs in console:

		docker-compose logs -f greyhound_app

5. After you did the next section and/or you want to change some code and start again, you'll need to shutdown the greyhound-app, kafka and zookeeper. To do so run this command:

		docker-compose down

### Produce and Consume messages

1. Navigate in your browser to http://localhost:8080
2. If you see `Hello Greyhound Application` we're good to go. If not, look for the cause in the logs or add debug logs to identify what's wrong (uncomment `debug: true` in the file `application.yml`)
3. Use curl
`curl -X GET 'localhost:8080/produce?numOfMessages=1000&maxParallelism=1'`
or navigate in the your browser to
http://localhost:8080/produce?numOfMessages=1000&maxParallelism=1
4. Check out the greyhound-app logs and wait for the last message to be consumed and the summery to appear:
`All messages consumed in 5704 millis at Mon Sep 21 04:58:32 GMT 2020`
5. Now try to increase the maxParallelism parameter to 8. Please note, the max value for parallelism is the number of partitions. In this example we configured the topic with 8 partitions (You can change that by changing the value of the constant `com.wixpress.dst.greyhound.getting.started.GreyhoundApplication#PARTITIONS` and following the steps above to launch the app
6. Use curl
`curl -X GET 'localhost:8080/produce?numOfMessages=1000&maxParallelism=8'`
or navigate in the your browser to
http://localhost:8080/produce?numOfMessages=1000&maxParallelism=8
7. Check out the greyhound-app logs and wait for the last message to be consumed and the summery to appear:
`All messages consumed in 765 millis at Mon Sep 21 05:07:41 GMT 2020`
8. **Notice the difference**, it takes 7.4 times less time to consume all the messages using greyhound feature of max parallelism!

### Credits
- The spring boot application is based on: [https://spring.io/guides/gs/spring-boot-docker/](https://spring.io/guides/gs/spring-boot-docker/)
- Docker compose is based on: [https://github.com/confluentinc/kafka-images/blob/master/examples/kafka-single-node/docker-compose.yml](https://github.com/confluentinc/kafka-images/blob/master/examples/kafka-single-node/docker-compose.yml)
