## Getting started with Greyhound and run it using Docker
This guild will walk you through the process of launching a simple greyhound application.
The app will be based on Spring Boot and Greyhound Java API.
We will create a Docker image from our app and run it together with a live Kafka, using docker compose.

### Prerequisites
 - A favorite text editor or IDE
 - [JDK 1.8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or later
 - [Gradle 4+](http://www.gradle.org/downloads) or [Maven 3.2+](https://maven.apache.org/download.cgi)
 - [Docker Compose](https://docs.docker.com/compose/install/)

### Steps to launch the app

1. Build greyhound app and the Docker image:

		cd /greyhound-app

	**Gradle**

		./gradlew bootBuildImage --imageName=wixpress/greyhound-app

	**Maven**

		./mvnw spring-boot:build-image -Dspring-boot.build-image.imageName=wixpress/greyhound-app

2. Start kafka, zookeeper and greyhound-app using docker-compose:

		cd ..
		docker-compose up -d
3. View greyhound-app logs in console

		docker-compose logs -f greyhound_app

### Produce and Consume messages

1. Navigate in your browser to `http://localhost:8080`
2.

### Credits
- The spring boot application is based on: [https://spring.io/guides/gs/spring-boot-docker/](https://spring.io/guides/gs/spring-boot-docker/)
- Docker compose is based on: [https://github.com/confluentinc/kafka-images/blob/master/examples/kafka-single-node/docker-compose.yml](https://github.com/confluentinc/kafka-images/blob/master/examples/kafka-single-node/docker-compose.yml)
