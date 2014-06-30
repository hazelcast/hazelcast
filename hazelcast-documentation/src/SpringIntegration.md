

## Spring Integration

### Supported Versions

- Spring 2.5+

### Spring Configuration

#### Classpath Configuration: 

Hazelcast-Spring integration requires one of the following jar in the classpath:

- hazelcast-spring-*version*.jar
- hazelcast-all-*version*.jar


#### Bean Declaration by Spring *beans* Namespace 

You can declare Hazelcast Objects using default Spring *beans* namespace. You can find an example usage of Hazelcast Instance declaration as follows:

```xml
<bean id="instance" class="com.hazelcast.core.Hazelcast" factory-method="newHazelcastInstance">
    <constructor-arg>
        <bean class="com.hazelcast.config.Config">
            <property name="groupConfig">
                <bean class="com.hazelcast.config.GroupConfig">
                    <property name="name" value="dev"/>
                    <property name="password" value="pwd"/>
                </bean>
            </property>
            <!-- and so on ... -->
        </bean>
    </constructor-arg>
</bean>

<bean id="map" factory-bean="instance" factory-method="getMap">
    <constructor-arg value="map"/>
</bean>
```

#### Bean Declaration by *hazelcast* Namespace 

Hazelcast has its own namespace **hazelcast** for bean definitions. You can easily add namespace *xmlns:hz="http://www.hazelcast.com/schema/spring"* to `beans` tag in context file so that *hz* namespace shortcut can be used bean declaration.

```xml
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xmlns:hz="http://www.hazelcast.com/schema/spring"
       xsi:schemaLocation="http://www.springframework.org/schema/beans
                http://www.springframework.org/schema/beans/spring-beans-2.5.xsd
                http://www.hazelcast.com/schema/spring
                http://www.hazelcast.com/schema/spring/hazelcast-spring-3.0.xsd">
```

#### Supported Configurations with *hazelcast* Namespace

-   Hazelcast Instance Configuration

```xml
<hz:hazelcast id="instance">
    <hz:config>
        <hz:group name="dev" password="password"/>
        <hz:network port="5701" port-auto-increment="false">
            <hz:join>
                <hz:multicast enabled="false"
                              multicast-group="224.2.2.3"
                              multicast-port="54327"/>
                <hz:tcp-ip enabled="true">
                    <hz:members>10.10.1.2, 10.10.1.3</hz:members>
                </hz:tcp-ip>
            </hz:join>
        </hz:network>
        <hz:map name="map"
                backup-count="2"
                max-size="0"
                eviction-percentage="30"
                read-backup-data="true"
                eviction-policy="NONE"
                merge-policy="com.hazelcast.map.merge.PassThroughMergePolicy"/>
    </hz:config>
</hz:hazelcast>
```

-   Hazelcast Client Configuration

```xml
<hz:client id="client">
     <hz:group name="${cluster.group.name}" password="${cluster.group.password}" />
     <hz:network connection-attempt-limit="3"
                            connection-attempt-period="3000"
                            connection-timeout="1000"
                            redo-operation="true"
                            smart-routing="true">
                    <hz:member>10.10.1.2:5701</hz:member>
                    <hz:member>10.10.1.3:5701</hz:member>
     </hz:network>
</hz:client>
```

-   Hazelcast Supported Type Configurations and Examples

-   `map`
-   `multiMap`
-   `replicatedmap`
-   `queue`
-   `topic`
-   `set`
-   `list`
-   `executorService`
-   `idGenerator`
-   `atomicLong`
-   `atomicReference`
-   `semaphore`
-   `countDownLatch`
-   `lock`


```xml
<hz:map id="map" instance-ref="client" name="map" lazy-init="true" />
<hz:multiMap id="multiMap" instance-ref="instance" name="multiMap" lazy-init="false" />
<hz:replicatedmap id="replicatedmap" instance-ref="instance" name="replicatedmap" lazy-init="false" />
<hz:queue id="queue" instance-ref="client" name="queue" lazy-init="true" depends-on="instance"/>
<hz:topic id="topic" instance-ref="instance" name="topic" depends-on="instance, client"/>
<hz:set id="set" instance-ref="instance" name="set" />
<hz:list id="list" instance-ref="instance" name="list"/>
<hz:executorService id="executorService" instance-ref="client" name="executorService"/>
<hz:idGenerator id="idGenerator" instance-ref="instance" name="idGenerator"/>
<hz:atomicLong id="atomicLong" instance-ref="instance" name="atomicLong"/>
<hz:atomicReference id="atomicReference" instance-ref="instance" name="atomicReference"/>
<hz:semaphore id="semaphore" instance-ref="instance" name="semaphore"/>
<hz:countDownLatch id="countDownLatch" instance-ref="instance" name="countDownLatch"/>
<hz:lock id="lock" instance-ref="instance" name="lock"/>
```

-   Supported Spring Bean Attributes

Hazelcast also supports `lazy-init`, `scope` and `depends-on` bean attributes.

```xml
<hz:hazelcast id="instance" lazy-init="true" scope="singleton">
    ...
</hz:hazelcast>

<hz:client id="client" scope="prototype" depends-on="instance">
    ...
</hz:client>
```

-   MapStore and NearCache Configuration

For map-store, you should set either *class-name* or *implementation* attribute

```xml
<hz:config>
    <hz:map name="map1">
            <hz:near-cache time-to-live-seconds="0" max-idle-seconds="60"
               eviction-policy="LRU" max-size="5000"  invalidate-on-change="true"/>

            <hz:map-store enabled="true" class-name="com.foo.DummyStore"
                write-delay-seconds="0"/>
    </hz:map>

    <hz:map name="map2">
            <hz:map-store enabled="true" implementation="dummyMapStore"
                write-delay-seconds="0"/>
    </hz:map>

    <bean id="dummyMapStore" class="com.foo.DummyStore" />
</hz:config>
```

### Spring Managed Context

It  is often desired to access Spring managed beans, to apply bean properties or to apply factory callbacks such as `ApplicationContextAware`, `BeanNameAware` or to apply bean post-processing such as `InitializingBean`, `@PostConstruct` like annotations while using Hazelcast distributed `ExecutorService` or more generally any Hazelcast managed object. Achieving those features are as simple as adding `@SpringAware` annotation to your distributed object types. Once you have configured HazelcastInstance as explained in [Spring Configuration](#spring-configuration) section, just mark any distributed type with `@SpringAware` annotation.

```xml
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xmlns:context="http://www.springframework.org/schema/context"
       xmlns:hz="http://www.hazelcast.com/schema/spring"
       xsi:schemaLocation="http://www.springframework.org/schema/beans
                http://www.springframework.org/schema/beans/spring-beans-3.0.xsd
                http://www.springframework.org/schema/context
                http://www.springframework.org/schema/context/spring-context-3.0.xsd
                http://www.hazelcast.com/schema/spring
                http://www.hazelcast.com/schema/spring/hazelcast-spring-3.3.xsd">

    <context:annotation-config />

    <hz:hazelcast id="instance">
        <hz:config>
            <hz:group name="dev" password="password"/>
            <hz:network port="5701" port-auto-increment="false">
                <hz:join>
                    <hz:multicast enabled="false" />
                    <hz:tcp-ip enabled="true">
                        <hz:members>10.10.1.2, 10.10.1.3</hz:members>
                    </hz:tcp-ip>
                </hz:join>
            </hz:network>
            ...
        </hz:config>
    </hz:hazelcast>

    <bean id="someBean" class="com.hazelcast.examples.spring.SomeBean" scope="singleton" />
    ...
</beans>
```

**ExecutorService example:**

```java
@SpringAware
public class SomeTask implements Callable<Long>, ApplicationContextAware, Serializable {

    private transient ApplicationContext context;

    private transient SomeBean someBean;

    public Long call() throws Exception {
        return someBean.value;
    }

    public void setApplicationContext(final ApplicationContext applicationContext)
        throws BeansException {
        context = applicationContext;
    }

    @Autowired
    public void setSomeBean(final SomeBean someBean) {
        this.someBean = someBean;
    }
}
```
```java
HazelcastInstance hazelcast = (HazelcastInstance) context.getBean("hazelcast");
SomeBean bean = (SomeBean) context.getBean("someBean");

Future<Long> f = hazelcast.getExecutorService().submit(new SomeTask());
Assert.assertEquals(bean.value, f.get().longValue());

// choose a member
Member member = hazelcast.getCluster().getMembers().iterator().next();

Future<Long> f2 = (Future<Long>) hazelcast.getExecutorService()
    .submitToMember(new SomeTask(), member);
Assert.assertEquals(bean.value, f2.get().longValue());
```

**Distributed Map value example:**

```java
@SpringAware
@Component("someValue")
@Scope("prototype")
public class SomeValue implements Serializable, ApplicationContextAware {

    transient ApplicationContext context;

    transient SomeBean someBean;

    transient boolean init = false;

    public void setApplicationContext(final ApplicationContext applicationContext)
        throws BeansException {
        context = applicationContext;
    }

    @Autowired
    public void setSomeBean(final SomeBean someBean) {
        this.someBean = someBean;
    }

    @PostConstruct
    public void init() {
        someBean.doSomethingUseful();
        init = true;
    }
    ...
}
```

On Node-1;

```java
HazelcastInstance hazelcast = (HazelcastInstance) context.getBean("hazelcast");
SomeValue value = (SomeValue) context.getBean("someValue")
IMap<String, SomeValue> map = hazelcast.getMap("values");
map.put("key", value);
```

On Node-2;

```java
HazelcastInstance hazelcast = (HazelcastInstance) context.getBean("hazelcast");
IMap<String, SomeValue> map = hazelcast.getMap("values");
SomeValue value = map.get("key");
Assert.assertTrue(value.init);
```

*Note that, Spring managed properties/fields are marked as `transient`.*



### Spring Cache

As of version 3.1, Spring Framework provides support for adding caching into an existing Spring application. To use Hazelcast as Spring cache provider, you should just define a `com.hazelcast.spring.cache.HazelcastCacheManager` bean and register it as Spring cache manager.

```xml
<cache:annotation-driven cache-manager="cacheManager" />

<hz:hazelcast id="hazelcast">
    ...
</hz:hazelcast>

<bean id="cacheManager" class="com.hazelcast.spring.cache.HazelcastCacheManager">
    <constructor-arg ref="instance"/>
</bean>
```

For more information please see [Spring Cache Abstraction](http://static.springsource.org/spring/docs/3.1.x/spring-framework-reference/html/cache.html).



### Hibernate 2nd Level Cache Config

If you are using Hibernate with Hazelcast as 2nd level cache provider, you can easily create `RegionFactory` instances within Spring configuration (by Spring version 3.1). That way, it is possible to use same `HazelcastInstance` as Hibernate L2 cache instance.

```xml
<hz:hibernate-region-factory id="regionFactory" instance-ref="instance" />
...
<bean id="sessionFactory" 
      class="org.springframework.orm.hibernate3.LocalSessionFactoryBean" 
	  scope="singleton">
    <property name="dataSource" ref="dataSource"/>
    <property name="cacheRegionFactory" ref="regionFactory" />
    ...
</bean>
```


### Best Practices

#### Out of Memory Error

Spring tries to create a new `Map`/`Collection` instance and fill the new instance by iterating and converting values of the original `Map`/`Collection` (`IMap`, `IQueue`, etc.) to required types when generic type parameters of the original `Map`/`Collection` and the target property/attribute do not match.

Since Hazelcast `Map`s/`Collection`s are designed to hold very large data which a single machine cannot carry, iterating through whole values can cause out of memory errors.

To avoid this issue, either target property/attribute can be declared as un-typed `Map`/`Collection` as shown below:

```java
public class SomeBean {
    @Autowired
    IMap map; // instead of IMap<K, V> map

    @Autowired
    IQueue queue; // instead of IQueue<E> queue

    ...
}
```

Or, parameters of injection methods (constructor, setter) can be un-typed as shown below:

```java
public class SomeBean {

    IMap<K, V> map;

    IQueue<E> queue;

    public SomeBean(IMap map) { // instead of IMap<K, V> map
        this.map = map;
    }

    ...

    public void setQueue(IQueue queue) { // instead of IQueue<E> queue
        this.queue = queue;
    }
    ...
}
```

For more information please see [Spring issue-3407](https://jira.springsource.org/browse/SPR-3407).

<br> </br>
