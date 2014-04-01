

## Spring Data - MongoDB

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

**Related Information**

For more information please see [Spring Data MongoDB Reference](http://static.springsource.org/spring-data/data-mongodb/docs/current/reference/html/).
