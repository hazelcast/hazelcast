## Spring Boot Integration

Hazelcast Jet provides its own Spring Boot 
[Starter](https://github.com/hazelcast/hazelcast-jet-contrib/tree/master/hazelcast-jet-spring-boot-starter).
You need to add the starter dependency to `pom.xml` and configuration
file (`hazelcast-jet.yaml`) to the project root or classpath. If you 
don't provide a configuration file, the starter will use the default 
configuration (`hazelcast-jet-default.yaml`). 

```java
@SpringBootApplication
public class SpringBootSample {

    @Autowired
    JetInstance instance;

    public static void main(String[] args) {
        SpringApplication.run(AppConfig.class, args);
    }
}
```