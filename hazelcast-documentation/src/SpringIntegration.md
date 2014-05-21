

## Spring Integration


### Configuration

***Note***: *Hazelcast-Spring integration requires either *`hazelcast-spring-`*version*`.jar`* or *`hazelcast-all-`*version*`.jar`* in the classpath.*

You can declare Hazelcast beans for Spring context using *beans* namespace (default Spring *beans* namespace) as well to declare Hazelcast maps, queues and others. 

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

Hazelcast has Spring integration (requires version 2.5 or greater) since 1.9.1 using *hazelcast* namespace.

-   Add namespace *xmlns:hz="http://www.hazelcast.com/schema/spring"* to `beans` tag in context file:

	```xml
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xmlns:hz="http://www.hazelcast.com/schema/spring"
       xsi:schemaLocation="http://www.springframework.org/schema/beans
                http://www.springframework.org/schema/beans/spring-beans-2.5.xsd
                http://www.hazelcast.com/schema/spring
                http://www.hazelcast.com/schema/spring/hazelcast-spring-3.0.xsd">
```

-   Use *hz* namespace shortcuts to declare cluster, its items and so on.

After that you can configure Hazelcast instance as shown below.

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

You can easily configure `map-store` and `near-cache`, too. For map-store, you should set either *class-name* or *implementation* attribute.)

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

It is possible to use placeholders instead of concrete values. For instance, use property file *app-default.properties* for group configuration:

```xml
<bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer">
    <property name="locations">
        <list>
            <value>classpath:/app-default.properties</value>
        </list>
    </property>
</bean>

<hz:hazelcast id="instance">
    <hz:config>
        <hz:group
            name="${cluster.group.name}"
            password="${cluster.group.password}"/>
        <!-- ... -->
    </hz:config>
</hz:hazelcast>
```

Similar for client:

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
Hazelcast also supports `lazy-init`, `scope` and `depends-on` bean attributes.

```xml
<hz:hazelcast id="instance" lazy-init="true" scope="singleton">
    ...
</hz:hazelcast>

<hz:client id="client" scope="prototype" depends-on="instance">
    ...
</hz:client>
```

You can declare beans for the following Hazelcast objects:

-   `map`
-   `multiMap`
-   `queue`
-   `topic`
-   `set`
-   `list`
-   `executorService`
-   `idGenerator`
-   `atomicLong`
-   `semaphore`
-   `countDownLatch`
-   `lock`

Example:

```xml
<hz:map id="map" instance-ref="client" name="map" lazy-init="true" />
<hz:multiMap id="multiMap" instance-ref="instance" name="multiMap" lazy-init="false" />
<hz:queue id="queue" instance-ref="client" name="queue" lazy-init="true" depends-on="instance"/>
<hz:topic id="topic" instance-ref="instance" name="topic" depends-on="instance, client"/>
<hz:set id="set" instance-ref="instance" name="set" />
<hz:list id="list" instance-ref="instance" name="list"/>
<hz:executorService id="executorService" instance-ref="client" name="executorService"/>
<hz:idGenerator id="idGenerator" instance-ref="instance" name="idGenerator"/>
<hz:atomicLong id="atomicLong" instance-ref="instance" name="atomicLong"/>
<hz:semaphore id="semaphore" instance-ref="instance" name="semaphore"/>
<hz:countDownLatch id="countDownLatch" instance-ref="instance" name="countDownLatch"/>
<hz:lock id="lock" instance-ref="instance" name="lock"/>
```



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
                http://www.hazelcast.com/schema/spring/hazelcast-spring-3.2.xsd">

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



### Spring Data - JPA

Hazelcast supports JPA persistence integrated with [Spring Data-JPA](http://www.springsource.org/spring-data/jpa) module. Your POJOs are mapped and persisted to your relational database. To use JPA persistence, first you should create a Repository interface extending `CrudRepository` class with object type that you want to persist.

```java
package com.hazelcast.jpa.repository;

import com.hazelcast.jpa.Product;
import org.springframework.data.repository.CrudRepository;

public interface ProductRepository extends CrudRepository<Product, Long> {

}
```

Then you should add your data source and repository definition to your Spring configuration, as shown below.

```xml
<jpa:repositories
       base-package="com.hazelcast.jpa.repository" />

    <bean class="com.hazelcast.jpa.SpringJPAMapStore" id="jpamapstore">
        <property name="crudRepository" ref="productRepository" />
    </bean>

    <bean class="org.apache.commons.dbcp.BasicDataSource" destroy-method="close" id="dataSource">
        <property name="driverClassName" value="com.mysql.jdbc.Driver"/>
             <property name="url" value="jdbc:mysql://localhost:3306/YOUR_DB"/>
             <property name="username" value="YOUR_USERNAME"/>
             <property name="password" value="YOUR_PASSWORD"/>
    </bean>

    <bean id="entityManagerFactory"
      class="org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean">
      <property name="dataSource" ref="dataSource" />
      <property name="jpaVendorAdapter">
        <bean class="org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter">
          <property name="generateDdl" value="true" />
          <property name="database" value="MYSQL" />
        </bean>
      </property>
        <property name="persistenceUnitName" value="jpa.sample" />
    </bean>

    <bean class="org.springframework.orm.jpa.JpaTransactionManager"
      id="transactionManager">
      <property name="entityManagerFactory"
          ref="entityManagerFactory" />
      <property name="jpaDialect">
        <bean class="org.springframework.orm.jpa.vendor.HibernateJpaDialect" />
      </property>
    </bean>
    
```

In the example configuration above, Hibernate and MySQL is configured. You change them according to your ORM and database selection. Also, you should define your persistence unit with `persistence.xml` under META-INF directory.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<persistence version="2.0" xmlns="http://java.sun.com/xml/ns/persistence" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://java.sun.com/xml/ns/persistence http://java.sun.com/xml/ns/persistence/persistence_2_0.xsd">
    <persistence-unit name="jpa.sample" />
</persistence>
```

By default, the key is expected to be the same with ID of the JPA object. You can change this behavior and customize MapStore implementation extending `SpringJPAMapStore` class. 

<br> </br>

<font color="red">
***Related Information***
</font>

*For more information please see [Spring Data JPA Reference](http://static.springsource.org/spring-data/data-jpa/docs/current/reference/html/).*




### Spring Data - MongoDB

Hazelcast supports MongoDB persistence integrated with [Spring Data-MongoDB](http://www.springsource.org/spring-data/mongodb) module. Spring MongoDB module maps your objects to equivalent MongoDB objects. To persist your objects into MongoDB, you should define MongoDB mapstore in your Spring configuration as follows:

```xml
<mongo:mongo id="mongo" host="localhost" port="27017"/>

<bean id="mongoTemplate"
      class="org.springframework.data.mongodb.core.MongoTemplate">
    <constructor-arg ref="mongo"/>
    <constructor-arg name="databaseName" value="test"/>
</bean>

<bean class="com.hazelcast.spring.mongodb.MongoMapStore" id="mongomapstore">
    <property name="mongoTemplate" ref="mongoTemplate" />
</bean>
```

Then, you can set this as map store for maps that you want to persist into MongoDB.

```xml
<hz:map name="user">
    <hz:map-store enabled="true" implementation="mongomapstore"
                  write-delay-seconds="0">
    </hz:map-store>
</hz:map>
```

By default, the key is set as id of the MongoDB object. You can override `MongoMapStore` class for you custom needs. 

<br> </br>

<font color="red">
***Related Information***
</font>

*For more information please see [Spring Data MongoDB Reference](http://static.springsource.org/spring-data/data-mongodb/docs/current/reference/html/).*

<br> </br>
