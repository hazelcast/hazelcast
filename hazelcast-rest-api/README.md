# Read Me

## Hazelcast Rest API
### API Details
* **Name**: hazelcast-rest-api
* **BaseUrl**: http://localhost:8080/
* **Swagger Page**: http://localhost:8080/swagger-ui/index.html
* **Java Version**: 17
* **Spring Boot Version**: 2.7.16

### Endpoints
The app defines following endpoints:
* **GET /hazelcast/rest/api/v1/cluster/members**: Query params: self(boolean), page(int), size(int).
* **GET /hazelcast/rest/api/v1/cluster/members/{member-uuid}**: Path params: member-uuid.

### Testing the Rest API: Step-by-Step Guide:

1. Create a Java project and add these dependencies to pom.xml file.

           <dependency>
              <groupId>com.hazelcast</groupId>
              <artifactId>hazelcast-rest-api</artifactId>
              <version>5.4.0-SNAPSHOT</version>
          </dependency>
          <dependency>
              <groupId>com.hazelcast</groupId>
              <artifactId>hazelcast</artifactId>
              <version>5.4.0-SNAPSHOT</version>
          </dependency>

2. Create a Config, and then create a HazelcastInstance with this config.

        Config config = new Config();

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

3. To access swagger UI, go to http://localhost:8080/swagger-ui/index.html