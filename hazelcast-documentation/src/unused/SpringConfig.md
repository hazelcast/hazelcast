
## Configuration

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

After that you can configure Hazelcast instance (node) as shown below.

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
<hz:client id="client"
    group-name="${cluster.group.name}" group-password="${cluster.group.password}">
    <hz:member>10.10.1.2:5701</hz:member>
    <hz:member>10.10.1.3:5701</hz:member>
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
