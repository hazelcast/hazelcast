## Spring Integration

This sample aims to show how to configure Hazelcast Jet 
as a Spring bean. There are two ways to achieve this:

1. [Programmatic](src/main/java/com/hazelcast/jet/examples/spring/AnnotationBasedConfigurationSample.java)
(annotation-based) configuration. This is the easiest way 
to configure Hazelcast Jet, simply create a 
[config class](src/main/java/com/hazelcast/jet/examples/spring/config/AppConfig.java)
which you can provide `JetInstance` as a bean with `@Bean` annotation.
Inside the method annotated with `@Bean` you can create and configure
`JetInstance` programmatically.

2. [Declarative](src/main/java/com/hazelcast/jet/examples/spring/XmlConfigurationWithSchemaSample.java)
(xml) configuration. You can configure Hazelcast Jet using 
an xml file too. Here you have 2 choices:
 
 - using spring core bean definitions.
 ([application-context.xml](src/main/resources/application-context.xml))
            
   - using Hazelcast Jet provided schema for bean definitions.
     ([application-context-with-schema.xml](src/main/resources/application-context-with-schema.xml))

    You should have hazelcast-jet-spring.jar in your classpath 
    to use Hazelcast Jet bean definitions. You will have 
    `<jet:instance>` and `<jet:client>` bean definitions to 
    create/configure Hazelcast Jet Instance and Hazelcast Jet 
    Client respectively. You will have other bean definitions like
    `<jet:map>`, `<jet:list>` and `<jet:hazelcast>` to obtain 
    `IMap`, `IListJet` and the underlying `HazelcastInstance` 
    as a spring bean.


## Spring Boot Integration

You can integrate Hazelcast Jet with Spring Boot easily. The approach is
very similar to the programmatic configuration. You need to have a
[config class](src/main/java/com/hazelcast/jet/examples/spring/config/AppConfig.java)
which you will use to start the spring application.

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

## @SpringAware annotation

Hazelcast Jet manages the life-cycle of processors itself, which
means creation and initialization of processors are done on each
member without any interference of spring context. If you want to
inject spring context to your processors you should mark them with
`@SpringAware` annotation. Here is a custom source 
[example](src/main/java/com/hazelcast/jet/examples/spring/source/CustomSourceP.java).

You have to configure Hazelcast Jet to use `SpringManagedContext`
to enable `@SpringAware` annotation. If configured, the `SpringManagedContext`
checks the processor for `@SpringAware` annotation. If the annotation 
is present then it will inject spring context.

```java
    @Bean
    public ManagedContext managedContext() {
        return new SpringManagedContext();
    }

    @Bean
    public JetInstance instance() {
        Config config = new Config()
                .setManagedContext(managedContext());
        JetConfig jetConfig = new JetConfig()
                .setHazelcastConfig(config);
        return Jet.newJetInstance(jetConfig);
    }
```

```xml
    <hz:config>
        ...
        <hz:spring-aware/>
        ...
    </hz:config>    
```