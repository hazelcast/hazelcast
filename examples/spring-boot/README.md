## Spring Boot Integration

You can integrate Hazelcast Jet with Spring Boot easily. The approach is
very similar to the programmatic configuration. You need to have a
[config class](../spring/src/main/java/com/hazelcast/jet/examples/spring/config/AppConfig.java)
which has `@SpringBootApplication` annotation.

```java
@RestController
public class SpringBootSample {

    @Autowired
    JetInstance instance;

    public static void main(String[] args) {
        SpringApplication.run(AppConfig.class, args);
    }

    @RequestMapping("/submitJob")
    public void submitJob() {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(CustomSourceP.customSource())
                .writeTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig()
                .addClass(SpringBootSample.class)
                .addClass(CustomSourceP.class);
        instance.newJob(pipeline, jobConfig).join();
    }

}
```