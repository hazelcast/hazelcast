---
title: Change Data Capture from MySQL
sidebar_label: MySQL
description: How to monitor Change Data Capture data from a MySQL database in Jet.
id: version-4.2-cdc
original_id: cdc
---

**Change Data Capture** (CDC) refers to the process of **observing
changes made to a database** and extracting them in a form usable by
other systems, for the purposes of replication, analysis and many more.
Basically anything that requires keeping multiple heterogeneous
datastores in sync.

CDC is especially important to Jet, because it allows for the
**streaming of changes from databases**, which can be efficiently
processed by Jet. Jet's implementation is based on
[Debezium](https://debezium.io/), which is an open source distributed
platform for change data capture.

Let's see an example, how to process change events in Jet, from a
MySQL database.

## 1. Install Docker

This tutorial uses [Docker](https://www.docker.com/) to simplify the
setup of a MySQL database, which you can freely experiment on.

1. Follow Docker's [Get Started](https://www.docker.com/get-started)
   instructions and install it on your system.
2. Test that it works:
   * Run `docker version` to check that you have the latest release
     installed.
   * Run `docker run hello-world` to verify that Docker is pulling
     images and running as expected.

## 2. Start MySQL Database

Open a terminal, and run following command. It will start a new
container that runs a MySQL database server preconfigured with an
inventory database:

```bash
docker run -it --rm --name mysql -p 3306:3306 \
    -e MYSQL_ROOT_PASSWORD=debezium -e MYSQL_USER=mysqluser \
    -e MYSQL_PASSWORD=mysqlpw debezium/example-mysql:1.2
```

This runs a new container using version `1.2` of the
[debezium/example-mysql](https://hub.docker.com/r/debezium/example-mysql)
image (based on [mysql:5.7](https://hub.docker.com/_/mysql)). It defines
and populates a sample "inventory" database and creates a `debezium`
user with password `dbz` that has the minimum privileges required by
Debezium’s MySQL connector.

The command assigns the name `mysql` to the container so that it can be
easily referenced later. The `-it` flag makes the container interactive,
meaning it attaches the terminal’s standard input and output to the
container so that you can see what is going on in the container. The
`--rm` flag instructs Docker to remove the container when it is stopped.

The command maps port `3306` (the default MySQL port) in the container
to the same port on the Docker host so that software outside of the
container can connect to the database server.

Finally, it also uses the `-e` option three times to set the
`MYSQL_ROOT_PASSWORD`, `MYSQL_USER`, and `MYSQL_PASSWORD` environment
variables to specific values.

You should see in your terminal something like the following:

```text
...
2020-03-09T09:48:24.579480Z 0 [Note] mysqld: ready for connections.
Version: '5.7.29-log'  socket: '/var/run/mysqld/mysqld.sock'  port: 3306  MySQL Community Server (GPL)
```

Notice that the MySQL server starts and stops a few times as the
configuration is modified. The last line listed above reports that the
MySQL server is running and ready for use.

## 3. Start MySQL Command Line Client

Open a new terminal, and use it to start a new container for the MySQL
command line client and connect it to the MySQL server running in the
`mysql` container:

```bash
docker run -it --rm --name mysqlterm --link mysql --rm mysql:5.7 sh \
    -c 'exec mysql -h"$MYSQL_PORT_3306_TCP_ADDR" -P"$MYSQL_PORT_3306_TCP_PORT" -uroot -p"$MYSQL_ENV_MYSQL_ROOT_PASSWORD"'
```

Here we start the container using the `mysql:5.7` image, name the
container `mysqlterm` and link it to the mysql container where the
database server is running.

The `--rm` option tells Docker to remove the container when it stops,
and the rest of the command defines the shell command that the container
should run. This shell command runs the MySQL command line client and
specifies the correct options so that it can connect properly.

The container should output lines similar to the following:

```text
mysql: [Warning] Using a password on the command line interface can be insecure.
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 4
Server version: 5.7.29-log MySQL Community Server (GPL)

Copyright (c) 2000, 2020, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql>
```

Unlike the other containers, this container runs a process that produces
a prompt. We’ll use the prompt to interact with the database. First,
switch to the "inventory" database:

```text
mysql> use inventory;
```

and then list the tables in the database:

```text
mysql> show tables;
```

which should then display:

```text
+---------------------+
| Tables_in_inventory |
+---------------------+
| addresses           |
| customers           |
| geom                |
| orders              |
| products            |
| products_on_hand    |
+---------------------+
6 rows in set (0.01 sec)
```

Use the MySQL command line client to explore the database and view the
pre-loaded data. For example:

```text
mysql> SELECT * FROM customers;
```

## 4. Start Hazelcast Jet

1. Download Hazelcast Jet

```bash
wget https://github.com/hazelcast/hazelcast-jet/releases/download/v4.2/hazelcast-jet-4.2.tar.gz
tar zxvf hazelcast-jet-4.2.tar.gz && cd hazelcast-jet-4.2
```

If you already have Jet and you skipped the above steps, make sure to
follow from here on.

2. Activate the MySQL CDC plugin:

```bash
mv opt/hazelcast-jet-cdc-debezium-4.2.jar lib; \
mv opt/hazelcast-jet-cdc-mysql-4.2.jar lib
```

3. Start Jet:

```bash
bin/jet-start
```

4. When you see output like this, Jet is up:

```text
Members {size:1, ver:1} [
    Member [192.168.1.5]:5701 - e7c26f7c-df9e-4994-a41d-203a1c63480e this
]
```

## 5. Create a New Java Project

We'll assume you're using an IDE. Create a blank Java project named
`cdc-tutorial` and copy the Gradle or Maven file into it:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
plugins {
    id 'com.github.johnrengelman.shadow' version '5.2.0'
    id 'java'
}

group 'org.example'
version '1.0-SNAPSHOT'

repositories.mavenCentral()

dependencies {
    compile 'com.hazelcast.jet:hazelcast-jet:4.2'
    compile 'com.hazelcast.jet:hazelcast-jet-cdc-debezium:4.2'
    compile 'com.hazelcast.jet:hazelcast-jet-cdc-mysql:4.2'
    compile 'com.fasterxml.jackson.core:jackson-annotations:2.11.0'
}

jar.manifest.attributes 'Main-Class': 'org.example.JetJob'
```

<!--Maven-->

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
   xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
   <modelVersion>4.0.0</modelVersion>

   <groupId>org.example</groupId>
   <artifactId>cdc-tutorial</artifactId>
   <version>1.0-SNAPSHOT</version>

   <properties>
       <maven.compiler.target>1.8</maven.compiler.target>
       <maven.compiler.source>1.8</maven.compiler.source>
   </properties>

   <dependencies>
       <dependency>
           <groupId>com.hazelcast.jet</groupId>
           <artifactId>hazelcast-jet</artifactId>
           <version>4.2</version>
       </dependency>
       <dependency>
           <groupId>com.hazelcast.jet</groupId>
           <artifactId>hazelcast-jet-cdc-debezium</artifactId>
           <version>4.2</version>
       </dependency>
       <dependency>
           <groupId>com.hazelcast.jet</groupId>
           <artifactId>hazelcast-jet-cdc-mysql</artifactId>
           <version>4.2</version>
       </dependency>
       <dependency>
           <groupId>com.fasterxml.jackson.core</groupId>
           <artifactId>jackson-annotations</artifactId>
           <version>2.11.0</version>
       </dependency>
   </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <configuration>
                    <archive>
                        <manifest>
                            <mainClass>org.example.JetJob</mainClass>
                        </manifest>
                    </archive>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

<!--END_DOCUSAURUS_CODE_TABS-->

## 6. Define Jet Job

Let's write the Jet code that will monitor the database and do something
useful with the data it sees. We will only monitor the `customers` table
and use the change events coming from it to maintain an up-to-date view
of all current customers.

By up-to-date view we mean an `IMap` keyed by customer ID and who's
values are `Customer` data objects containing all information for a
customer with a specific ID.

This is how the code doing this looks like:

```java
package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.cdc.CdcSinks;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;

public class JetJob {

    public static void main(String[] args) {
        StreamSource<ChangeRecord> source = MySqlCdcSources.mysql("source")
                .setDatabaseAddress("127.0.0.1")
                .setDatabasePort(3306)
                .setDatabaseUser("debezium")
                .setDatabasePassword("dbz")
                .setClusterName("dbserver1")
                .setDatabaseWhitelist("inventory")
                .setTableWhitelist("inventory.customers")
                .build();

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(source)
                .withoutTimestamps()
                .peek()
                .writeTo(CdcSinks.map("customers",
                        r -> r.key().toMap().get("id"),
                        r -> r.value().toObject(Customer.class).toString()));

        JobConfig cfg = new JobConfig().setName("mysql-monitor");
        Jet.bootstrappedInstance().newJob(pipeline, cfg);
    }

}
```

The `Customer` class we map change events to is quite simple too:

```java
package org.example;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

public class Customer implements Serializable {

    @JsonProperty("id")
    public int id;

    @JsonProperty("first_name")
    public String firstName;

    @JsonProperty("last_name")
    public String lastName;

    @JsonProperty("email")
    public String email;

    public Customer() {
    }

    public Customer(int id, String firstName, String lastName, String email) {
        super();
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
    }

    @Override
    public int hashCode() {
        return Objects.hash(email, firstName, id, lastName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Customer other = (Customer) obj;
        return id == other.id
                && Objects.equals(firstName, other.firstName)
                && Objects.equals(lastName, other.lastName)
                && Objects.equals(email, other.email);
    }

    @Override
    public String toString() {
        return "Customer {id=" + id + ", firstName=" + firstName + ", lastName=" + lastName + ", email=" + email + '}';
    }
}
```

To make it evident that our pipeline serves the purpose of building an
up-to-date cache of customers, which can be interrogated at any time
let's add one more class. This code can be executed at any time in your
IDE and will print the current content of the cache.

```java
package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;

public class CacheRead {

    public static void main(String[] args) {
        JetInstance instance = Jet.newJetClient();

        System.out.println("Currently there are following customers in the cache:");
        instance.getMap("customers").values().forEach(c -> System.out.println("\t" + c));

        instance.shutdown();
    }

}
```

## 7. Package

Now that we have all the pieces, we need to submit it to Jet for
execution. Since Jet runs on our machine as a standalone cluster in a
standalone process we need to give it all the code that we have written.

For this reason we create a jar containing everything we need. All we
need to do is to run the build command:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
gradle build
```

This will produce a jar file called `cdc-tutorial-1.0-SNAPSHOT.jar`
in the `build/libs` folder of our project.

<!--Maven-->

```bash
mvn package
```

This will produce a jar file called `cdc-tutorial-1.0-SNAPSHOT.jar`
in the `target` folder or our project.

<!--END_DOCUSAURUS_CODE_TABS-->

## 8. Submit for Execution

Assuming our cluster is [still running](#4-start-hazelcast-jet
) and the database [is up](#2-start-mysql-database), all we need to
issue is following command:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```bash
<path_to_jet>/bin/jet submit build/libs/cdc-tutorial-1.0-SNAPSHOT.jar
```

<!--Maven-->

```bash
<path_to_jet>/bin/jet submit target/cdc-tutorial-1.0-SNAPSHOT.jar
```

<!--END_DOCUSAURUS_CODE_TABS-->

The output in the Jet member's log should look something like this (we
also log what we put in the `IMap` sink thanks to the `peek()` stage
we inserted):

```text
... Completed snapshot in 00:00:01.519
... Output to ordinal 0: key:{{"id":1001}}, value:{{"id":1001,"first_name":"Sally","last_name":"Thomas",...
... Output to ordinal 0: key:{{"id":1002}}, value:{{"id":1002,"first_name":"George","last_name":"Bailey",...
... Output to ordinal 0: key:{{"id":1003}}, value:{{"id":1003,"first_name":"Edward","last_name":"Walker",...
... Output to ordinal 0: key:{{"id":1004}}, value:{{"id":1004,"first_name":"Anne","last_name":"Kretchmar",...
... Transitioning from the snapshot reader to the binlog reader
```

## 9. Track Updates

Let's see how our cache looks like at this time. If we execute the
 `CacheRead` code [defined above](#6-define-jet-job), we'll get:

```text
Currently there are following customers in the cache:
    Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}
    Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}
    Customer {id=1004, firstName=Anne, lastName=Kretchmar, email=annek@noanswer.org}
    Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}
```

Let's do some updates in our database. Go to the MySQL CLI
[we've started earlier](#3-start-mysql-command-line-client) and run
following update statement:

```text
mysql> UPDATE customers SET first_name='Anne Marie' WHERE id=1004;
Query OK, 1 row affected (0.00 sec)
Rows matched: 1  Changed: 1  Warnings: 0
```

In the log of the Jet member we should immediately see the effect:

```text
... Output to ordinal 0: key:{{"id":1004}}, value:{{"id":1004,"first_name":"Anne Marie","last_name":"Kretchmar",...
```

If we check the cache with `CacheRead` we get:

```text
Currently there are following customers in the cache:
    Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}
    Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}
    Customer {id=1004, firstName=Anne Marie, lastName=Kretchmar, email=annek@noanswer.org}
    Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}
```

One more:

```text
mysql> UPDATE customers SET email='edward.walker@walker.com' WHERE id=1003;
Query OK, 1 row affected (0.00 sec)
Rows matched: 1  Changed: 1  Warnings: 0
```

```text
Currently there are following customers in the cache:
    Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}
    Customer {id=1003, firstName=Edward, lastName=Walker, email=edward.walker@walker.com}
    Customer {id=1004, firstName=Anne Marie, lastName=Kretchmar, email=annek@noanswer.org}
    Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}
```

## 10. Clean up

Let's clean-up after ourselves. First we cancel our Jet job:

```bash
<path_to_jet>/bin/jet cancel mysql-monitor
```

Then we shut down our Jet member/cluster:

```bash
<path_to_jet>/bin/jet-stop
```

You can use Docker to stop all running containers:

```bash
docker stop mysqlterm mysql
```

Again, since we've used the `--rm` flag when starting the connectors,
Docker should remove them right after we stop them.
We can verify that all processes are stopped and removed with following
command:

```bash
docker ps -a
```
