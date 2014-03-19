

## Spring Data - JPA

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

**Related Information**

For more information please see [Spring Data JPA Reference](http://static.springsource.org/spring-data/data-jpa/docs/current/reference/html/).
