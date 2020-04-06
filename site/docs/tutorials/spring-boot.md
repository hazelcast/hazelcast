---
title: Spring Boot Starter 
description: How to auto-configure Jet in Spring Boot Application
---

Spring Boot makes it easy to create and use third-party libraries, such
as Hazelcast Jet, with minimum configurations possible. While Spring
Boot provides starters for some libraries, Hazelcast Jet hosts its own
[starter](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/hazelcast-jet-spring-boot-starter).

Let's create a simple Spring Boot application which starts a Jet
instance and auto-wires it.

## 1. Create a New Java Project

We assume you're using an IDE. Create a blank Java project named
`tutorial-jet-starter` and copy the Gradle or Maven file into it:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
plugins {
  id 'org.springframework.boot' version '2.2.6.RELEASE'
  id 'io.spring.dependency-management' version '1.0.9.RELEASE'
  id 'java'
}
group = 'org.example'
version '1.0-SNAPSHOT'

repositories.mavenCentral()

dependencies {
  compile 'com.hazelcast.jet.contrib:hazelcast-jet-spring-boot-starter:2.0.0'  
}
```

<!--Maven-->

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>2.2.6.RELEASE</version>
        <relativePath/>
    </parent>

    <groupId>org.example</groupId>
    <artifactId>tutorial-jet-starter</artifactId>
    <version>1.0-SNAPSHOT</version>

    <dependencies>
        <dependency>
            <groupId>com.hazelcast.jet.contrib</groupId>
            <artifactId>hazelcast-jet-spring-boot-starter</artifactId>
            <version>2.0.0</version>
        </dependency>
    </dependencies>

</project>
```

<!--END_DOCUSAURUS_CODE_TABS-->

## 2. Create the Application Main Class

The following code creates a Spring Boot application which starts a Jet
member with default configuration.

```java
package org.example;

import com.hazelcast.jet.JetInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TutorialApplication {

    @Autowired
    JetInstance jetInstance;

    public static void main(String[] args) {
        SpringApplication.run(TutorialApplication.class, args);
    }
}
```

When you run it on your IDE, you should see in the logs that a Jet
member is started and the default configuration file is used:

```text
...
c.h.i.config.AbstractConfigLocator       : Loading 'hazelcast-jet-default.xml' from the classpath.
...
com.hazelcast.core.LifecycleService      : [127.0.0.1]:5701 [jet] [4.0] [127.0.0.1]:5701 is STARTED
...
```

## 3. Custom Configuration

Let's add some custom configuration to our Jet member by defining a
configuration file named `hazelcast-jet.yaml` at the root directory.

```yaml
hazelcast-jet:
  instance:
    cooperative-thread-count: 4
  edge-defaults:
    queue-size: 2048
```

When you stop and re-run the main class you should now see that the
configuration file we've just created is used to start the member:

```text
...
c.h.i.config.AbstractConfigLocator       : Loading 'hazelcast-jet.yaml' from the working directory.
...
```

### Using Properties File

If your configuration file is not at the root directory or you want to
use a different name then you can create an `application.properties`
file and set the `hazelcast.jet.config` like below:

```properties
hazelcast.jet.config=file:config/hazelcast-jet-tutorial.yaml
```

### Using System Properties

You can also set configuration file using system property:

```java
System.setProperty("hazelcast.jet.config", "file:config/hazelcast-jet-tutorial.yaml");
```

## 4. Jet Client

If you have a Jet cluster already running and want to connect to it
with a client all you need to do is to put a client configuration file
(`hazelcast-client.yaml`) to the root directory instead of the Jet
configuration:

```yaml
hazelcast-client:
  cluster-name: tutorial-jet-starter
  network:
    cluster-members:
      - 127.0.0.1
```

### Using Properties File

If your configuration file is not at the root directory or you want to
use a different name then you can create an `application.properties`
file and set the `hazelcast.jet.client.config` like below:

```properties
hazelcast.jet.client.config=file:config/hazelcast-client-tutorial.yaml
```

### Using System Properties

You can also set configuration file using system property:

```java
System.setProperty("hazelcast.client.config", "file:config/hazelcast-client-tutorial.yaml");