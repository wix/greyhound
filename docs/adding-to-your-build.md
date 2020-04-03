## Adding Greyhound to your build

### Condigure Bintray Repo
##### Maven
```xml
<?xml version="1.0" encoding="UTF-8" ?>
<settings xsi:schemaLocation='http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd'
          xmlns='http://maven.apache.org/SETTINGS/1.0.0' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>
    
    <profiles>
        <profile>
            <repositories>
                <repository>
                    <snapshots>
                        <enabled>false</enabled>
                    </snapshots>
                    <id>bintray-dkarlinsky-greyhound</id>
                    <name>bintray</name>
                    <url>https://dl.bintray.com/dkarlinsky/greyhound</url>
                </repository>
            </repositories>
            <id>bintray</id>
        </profile>
    </profiles>
    <activeProfiles>
        <activeProfile>bintray</activeProfile>
    </activeProfiles>
</settings>
```
##### Gradle
```groovy
repositories {
    mavenCentral()    
    maven {
        url  "https://dl.bintray.com/dkarlinsky/greyhound" 
    }
}
```

### Greyhound - Scala Futures
##### Maven: 
```xml
<dependency>
  <groupId>com.wix.greyhound</groupId>
  <artifactId>greyhound-scala-futures_2.12</artifactId>
  <version>0.1.1</version>
  <type>pom</type>
</dependency>
```
##### Gradle:
```groovy
implementation 'com.wix.greyhound:greyhound-scala-futures_2.12:0.1.1'
```

### Greyhound - Java
##### Maven: 
```xml
<dependency>
  <groupId>com.wix.greyhound</groupId>
  <artifactId>greyhound-java_2.12</artifactId>
  <version>0.1.1</version>
  <type>pom</type>
</dependency>
```
##### Gradle:
```groovy
implementation 'com.wix.greyhound:greyhound-java_2.12:0.1.1'
```
