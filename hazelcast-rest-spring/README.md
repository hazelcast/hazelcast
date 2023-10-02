# Read Me

## Hazelcast Rest API
### API Details
* **Name**: hazelcast-rest-spring
* **BaseUrl**: http://localhost:8080/hazelcast/rest/
* **Swagger Page**: http://localhost:8080/hazelcast/rest/swagger-ui/index.html
* **Java Version**: 17
* **Spring Boot Version**: 2.7.16

### Description
This is a RESTful API using Spring Boot that provides various endpoints for managing resources. It includes Bearer Token with JWT Authentication for securing access to protected endpoints and integrates Swagger UI for easy API documentation and testing.

#### Comparing Spring Boot and Jersey
* <u>**Jersey:**</u> is an open source framework for developing RESTful Web Services. It serves as a reference implementation of JAX-RS (Java API for RESTful Web Services) specification.
* <u>**Spring Boot:**</u> is an open-source tool that makes it easier to use Java-based frameworks to create microservices and web apps. It provides a good support to building RESTful Web Services.


* <u>Pros of Spring Boot:</u>
  * Spring is a comprehensive framework that offers a wide range of features beyond just RESTful web services, such as dependency injection, security, testing and more.
  * Spring REST leverages the power of DI (Dependency Injection), making it easier to manage components and dependencies with the API.
  * Spring's architecture promotes testability, allowing you to write unit tests and integration tests with ease.


* <u>Cons of Spring Boot:</u>
  * Spring REST may require more configuration compared to lightweight frameworks.


* <u>Pros of Jersey:</u>
  * It's relatively lightweight compared to Spring Boot, making it suitable for resource-constrained environments.


* <u>Cons of Jersey:</u>
  * JAX-RS does not provide the same level of ecosystem and integrations as Spring Boot, requiring more manual setup for certain features.

<u>**Why We Chose Spring Boot 2 Over Spring Boot 3**</u>
* We went with Spring Boot 2 because Spring Boot 3 had some dependencies that caused issues with "Rule 1: org.codehaus.mojo.extraenforcer.dependencies.EnforceBytecodeVersion".

      [ERROR] Failed to execute goal org.apache.maven.plugins:maven-enforcer-plugin:3.4.0:enforce (enforce-tools) on project hazelcast-rest-spring:
      [ERROR] Rule 1: org.codehaus.mojo.extraenforcer.dependencies.EnforceBytecodeVersion failed with message:
      [ERROR] Found Banned Dependency: org.springframework:spring-webmvc:jar:6.0.11
      [ERROR] Found Banned Dependency: org.springframework:spring-expression:jar:6.0.11
      [ERROR] Found Banned Dependency: org.springframework:spring-context:jar:6.0.11
      [ERROR] Found Banned Dependency: org.springframework:spring-beans:jar:6.0.11
      [ERROR] Found Banned Dependency: org.springframework.boot:spring-boot-autoconfigure:jar:3.1.3
      [ERROR] Found Banned Dependency: org.springframework:spring-web:jar:6.0.11
      [ERROR] Found Banned Dependency: org.springframework.boot:spring-boot:jar:3.1.3


### Endpoints
The app defines following endpoints:
* **POST /hazelcast/rest/token**: Get a bearer token with a 10-minute expiration time.
* **GET /hazelcast/rest/cluster**: Check the current status of the cluster.
* **GET /hazelcast/rest/maps/{mapName}/{key}**: Retrieve a value from a map using a specific key.
* **POST /hazelcast/rest/maps/{mapName}/{key}**: Add a new value to the map using a specific key.

### Testing the Rest API: Step-by-Step Guide:

1. Create a Java project and add these dependencies to pom.xml file.

           <dependency>
              <groupId>com.hazelcast</groupId>
              <artifactId>hazelcast-rest-spring</artifactId>
              <version>5.4.0-SNAPSHOT</version>
          </dependency>
          <dependency>
              <groupId>com.hazelcast</groupId>
              <artifactId>hazelcast</artifactId>
              <version>5.4.0-SNAPSHOT</version>
          </dependency>
          <dependency>
              <groupId>com.hazelcast</groupId>
              <artifactId>hazelcast-enterprise</artifactId>
              <version>5.4.0-SNAPSHOT</version>
              <scope>compile</scope>
          </dependency>

2. Configure a SecurityConfig by enabling it and associating it with a member realm.

        SecurityConfig securityConfig = new SecurityConfig();
        securityConfig.setEnabled(true);
        securityConfig.setMemberRealm("memberRealm");

3. Create a RealmConfig and put a username and password in it using UsernamePasswordIdentityConfig.

        RealmConfig realmConfig = new RealmConfig();
        realmConfig.setUsernamePasswordIdentityConfig("Name", "Password");
        securityConfig.setMemberRealmConfig("memberRealm", realmConfig);

4. Create a Config and make a setup with your created securityConfig, remember to add a license key, and then create a HazelcastInstance with this setup.

        Config config = new Config();
        config.setSecurityConfig(securityConfig);
        config.setLicenseKey(""); //set license key

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

5. After starting the app, rest API will be accessible at http://localhost:8080/hazelcast/rest
6. To access swagger UI, go to http://localhost:8080/hazelcast/rest/swagger-ui/index.html
7. To get a token for accessing endpoints, send a request to http://localhost:8080/hazelcast/rest/token and include the acquired token in the "Authentication" header. 
  When using the Swagger page, insert the acquired token in the Authorize section in the UI to access all endpoints without the need to add the token manually as a header. 