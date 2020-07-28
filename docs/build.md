## Adding greyhound to your build
All Greyhound modules can be found in [Maven Central Repository](https://search.maven.org/search?q=greyhound).

### Greyhound Scala (Future) API
To add a dependency on greyhound-future using **Sbt**, use the following:

```sbt
libraryDependencies ++= Seq(
  "com.wix" % "greyhound-future_2.12" % "0.1.0",
  // other dependencies separated by commas
)
```

To add a dependency using **Maven**:

```xml
<dependency>
  <groupId>com.wix</groupId>
  <artifactId>greyhound-future_2.12</artifactId>
  <version>0.0.2</version>
</dependency>
```

To add a dependency using **Gradle**:

```gradle
dependencies {
  implementation("com.wix.guava:greyhound-future_2.12:0.1.0")
  // other dependencies
}
```

### Greyhound Java API
To add a dependency on greyhound-java using **Maven**, use the following:

```xml
<dependency>
  <groupId>com.wix</groupId>
  <artifactId>greyhound-java</artifactId>
  <version>0.0.2</version>
</dependency>
```

To add a dependency using **Gradle**:

```gradle
dependencies {
  implementation("com.wix.guava:greyhound-java:0.1.0")
  // other dependencies
}
```

### Greyhound ZIO API
To add a dependency on greyhound with zio using **Sbt**, use the following:

```sbt
libraryDependencies ++= Seq(
  "com.wix" % "greyhound-core_2.12" % "0.1.0",
  // other dependencies separated by commas
)
```

To add a dependency using **Maven**:

```xml
<dependency>
  <groupId>com.wix</groupId>
  <artifactId>greyhound-core_2.12</artifactId>
  <version>0.0.2</version>
</dependency>
```

To add a dependency using **Gradle**:

```gradle
dependencies {
  implementation("com.wix.guava:greyhound-core_2.12:0.1.0")
  // other dependencies
}
```
