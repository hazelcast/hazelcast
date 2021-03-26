---
title: Join Change Data Capture Records
sidebar_label: Join
description: How to maintain a joined image of Change Data Capture data from multiple tables.
id: version-4.4-cdc-join
original_id: cdc-join
---

**Change Data Capture** (CDC) refers to the process of **observing
changes made to a database** and extracting them in a form usable by
other systems, for the purposes of replication, analysis and many more.
Basically anything that requires keeping multiple heterogeneous
datastores in sync.

CDC is especially important to Jet, because it makes possible the
streaming of changes from databases, in a form efficiently processable
by Jet. Jet's implementation is based on [Debezium](https://debezium.io/),
which is an open source distributed platform for change data capture.

In our previous tutorials on how to extract change event data from
[MySQL](cdc.md) and [Postgres](cdc-postgres.md) databases, we've seen
how to monitor single database tables and how to maintain up-to-date
images of those tables in memory, in the form of maps.

Here we will examine how to make our map hold enriched data, combined
(joined, if you will) from multiple tables. Let's, for example, make
each of our enriched data objects hold information about one customer
and all the orders belonging to them.

## 1. Install Docker

This tutorial uses [Docker](https://www.docker.com/) to simplify the
setup of databases, which you can freely experiment on.

1. Follow Docker's [Get Started](https://www.docker.com/get-started)
   instructions and install it on your system.
2. Test that it works:
   * Run `docker version` to check that you have the latest release
     installed.
   * Run `docker run hello-world` to verify that Docker is pulling
     images and running as expected.

## 2. Start Database

Exact instructions for starting the supported databases are as follows:

* [MySQL](cdc.md#2-start-mysql-database)
* [Postgres](cdc-postgres.md#2-start-postgresql-database).

## 3. Start Command Line Client

Exact instructions for starting the specific command line client of the
supported databases are as follows:

* [MySQL](cdc.md#3-start-mysql-command-line-client)
* [Postgres](cdc-postgres.md#3-start-postgresql-command-line-client)

## 4. Create a New Java Project

We'll assume you're using an IDE. Create a blank Java project named
`cdc-tutorial` and copy the provided Gradle or Maven file into it.
They differ slightly depending on which database you use:

* [MySQL](cdc.md#5-create-a-new-java-project)
* [Postgres](cdc-postgres.md#5-create-a-new-java-project)

## 5. Define Jet Job

Let's write the Jet code for the processing we want to accomplish:

```java
package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.mysql.MySqlCdcSources;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;

public class JetJob {

    private static final int MAX_CONCURRENT_OPERATIONS = 1;

    public static void main(String[] args) {
        StreamSource<ChangeRecord> source = MySqlCdcSources.mysql("source")
            .setDatabaseAddress("127.0.0.1")
            .setDatabasePort(3306)
            .setDatabaseUser("debezium")
            .setDatabasePassword("dbz")
            .setClusterName("dbserver1")
            .setDatabaseWhitelist("inventory")
            .setTableWhitelist("inventory.customers", "inventory.orders")
            .build();

        Pipeline pipeline = Pipeline.create();
        StreamStage<ChangeRecord> allRecords = pipeline.readFrom(source)
                .withNativeTimestamps(0);

        allRecords.filter(r -> r.table().equals("customers"))
                .apply(Ordering::fix)
                .peek()
                .writeTo(Sinks.mapWithEntryProcessor(MAX_CONCURRENT_OPERATIONS, "cache",
                        record -> (Integer) record.key().toMap().get("id"),
                        CustomerEntryProcessor::new
                ));

        allRecords.filter(r -> r.table().equals("orders"))
                .apply(Ordering::fix)
                .peek()
                .writeTo(Sinks.mapWithEntryProcessor(MAX_CONCURRENT_OPERATIONS, "cache",
                        record -> (Integer) record.value().toMap().get("purchaser"),
                        OrderEntryProcessor::new
                ));

        JobConfig cfg = new JobConfig().setName("monitor");
        Jet.bootstrappedInstance().newJob(pipeline, cfg);
    }

}
```

If using Postgres, only the source would need to change, like this:

```java
StreamSource<ChangeRecord> source = PostgresCdcSources.postgres("source")
    .setDatabaseAddress("127.0.0.1")
    .setDatabasePort(5432)
    .setDatabaseUser("postgres")
    .setDatabasePassword("postgres")
    .setDatabaseName("postgres")
    .setTableWhitelist("inventory.customers", "inventory.orders")
    .build();
```

As we can see from the pipeline code, our `Sink` is `EntryProcessor`
based. The two `EntryProcessors` we use are:

```java
package org.example;

import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.map.EntryProcessor;

import java.util.Map;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class CustomerEntryProcessor implements EntryProcessor<Integer, OrdersOfCustomer, Object> {

    private final Customer customer;

    public CustomerEntryProcessor(ChangeRecord record) {
        try {
            this.customer = Operation.DELETE.equals(record.operation()) ? null :
                    record.value().toObject(Customer.class);
        } catch (ParsingException e) {
            throw rethrow(e);
        }
    }

    @Override
    public Object process(Map.Entry<Integer, OrdersOfCustomer> entry) {
        OrdersOfCustomer value = entry.getValue();
        if (customer == null) {
            if (value != null) {
                value.setCustomer(null);
            }
        } else {
            if (value == null) {
                value = new OrdersOfCustomer();
            }
            value.setCustomer(customer);
        }
        entry.setValue(value);
        return null;
    }
}
```

```java
package org.example;

import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.Operation;
import com.hazelcast.jet.cdc.ParsingException;
import com.hazelcast.map.EntryProcessor;

import java.util.Map;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class OrderEntryProcessor implements EntryProcessor<Integer, OrdersOfCustomer, Object> {

    private final Operation operation;
    private final Order order;

    public OrderEntryProcessor(ChangeRecord record) {
        try {
            this.order = record.value().toObject(Order.class);
            this.operation = record.operation();
        } catch (ParsingException e) {
            throw rethrow(e);
        }
    }

    @Override
    public Object process(Map.Entry<Integer, OrdersOfCustomer> entry) {
        OrdersOfCustomer value = entry.getValue();
        if (Operation.DELETE.equals(operation)) {
            if (value != null) {
                value.deleteOrder(order);
            }
        } else {
            if (value == null) {
                value = new OrdersOfCustomer();
            }
            value.addOrUpdateOrder(order);
        }
        entry.setValue(value);
        return null;
    }
}
```

In them we use the `Customer` and the `Order` classes to achieve
convenient data parsing with the help of data to object
mapping.

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

    Customer() {
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

```java
package org.example;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class Order implements Serializable {

    @JsonProperty("order_number")
    public int orderNumber;

    @JsonProperty("order_date")
    public Date orderDate;

    @JsonProperty("purchaser")
    public int purchaser;

    @JsonProperty("quantity")
    public int quantity;

    @JsonProperty("product_id")
    public int productId;

    Order() {
    }

    public void setOrderDate(Date orderDate) { //used by object mapping
        long days = orderDate.getTime();
        this.orderDate = new Date(TimeUnit.DAYS.toMillis(days));
    }

    public int getOrderNumber() {
        return orderNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderNumber, orderDate, purchaser, quantity, productId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Order other = (Order) obj;
        return orderNumber == other.orderNumber
                && Objects.equals(orderDate, other.orderDate)
                && Objects.equals(purchaser, other.purchaser)
                && Objects.equals(quantity, other.quantity)
                && Objects.equals(productId, other.productId);
    }

    @Override
    public String toString() {
        return "Order {orderNumber=" + orderNumber + ", orderDate=" + orderDate + ", purchaser=" + purchaser +
                ", quantity=" + quantity + ", productId=" + productId + '}';
    }

}
```

Watch out, in the Postgres database the order number column has a
different name, `id`, so the first field in `Order` needs to be changed
to:

```java
@JsonProperty("id")
public int orderNumber;
```

Besides these two data classes we also need to define our enriched
structure, called `OrdersOfCustomers`, which will be stored in the
target `IMap`:

```java
package org.example;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class OrdersOfCustomer implements Serializable {

    private final Map<Integer, Order> orders;
    private Customer customer;

    public OrdersOfCustomer() {
        this.customer = null;
        this.orders = new HashMap<>();
    }

    public void setCustomer(Customer customer) {
        this.customer = customer;
    }

    public void deleteOrder(Order order) {
        orders.remove(order.getOrderNumber());
    }

    public void addOrUpdateOrder(Order order) {
        orders.put(order.getOrderNumber(), order);
    }

    @Override
    public int hashCode() {
        return Objects.hash(customer, orders);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        OrdersOfCustomer other = (OrdersOfCustomer) obj;
        return Objects.equals(customer, other.customer)
                && Objects.equals(orders, other.orders);
    }

    @Override
    public String toString() {
        return String.format("Customer: %s, Orders: %s", customer, orders);
    }
}
```

There is also another element in the pipeline, an extra processing stage
which handles and fixes event reordering that might happen due to
parallel processing. It's based on sequence numbers specific to CDC
sources and so can be used only for these kinds of pipelines. Hopefully
a future version of Jet will introduce a generic solution for the
reordering problem.

```java
package org.example;

import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.RecordPart;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.concurrent.TimeUnit;

public class Ordering {

    static StreamStage<ChangeRecord> fix(StreamStage<ChangeRecord> input) {
        return input
                .groupingKey(ChangeRecord::key)
                .mapStateful(
                        TimeUnit.SECONDS.toMillis(10),
                        () -> new LongAccumulator(0),
                        (lastSequence, key, record) -> {
                            long sequence = record.sequenceValue();
                            if (lastSequence.get() < sequence) {
                                lastSequence.set(sequence);
                                return record;
                            }
                            return null;
                        },
                        (TriFunction<LongAccumulator, RecordPart, Long, ChangeRecord>) (sequence, recordPart, aLong) -> null);
    }

}
```

To make it evident that our pipeline serves the purpose of building an
up-to-date cache of "orders of customers", which can be interrogated at
any time, let's add one more class. This code can be executed at will in
your IDE and prints the current content of the cache.

```java
package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;

public class CacheRead {

    public static void main(String[] args) {
        JetInstance instance = Jet.newJetClient();

        System.out.println("Currently there are following customers in the cache:");
        instance.getMap("cache").values().forEach(c -> System.out.println("\t" + c));

        instance.shutdown();
    }

}
```

## 6. Package

Now that we have defined all the pieces, we need to submit the
pipeline to Jet for execution. Since Jet runs on our machine as a
standalone cluster in a standalone process we need to make it aware of
all the code that we have written.

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

## 7. Start Hazelcast Jet

1. Download Hazelcast Jet

```bash
wget https://github.com/hazelcast/hazelcast-jet/releases/download/v4.4/hazelcast-jet-4.4.tar.gz
tar zxvf hazelcast-jet-4.4.tar.gz && cd hazelcast-jet-4.4
```

2. Make sure the CDC plugin for the database is in the `lib/` directory.

```bash
ls lib/
```

You should see the following jars:

* hazelcast-jet-cdc-debezium-4.4.jar
* hazelcast-jet-cdc-mysql-4.4.jar (for MySQL)
* hazelcast-jet-cdc-postgres-4.4.jar (for Postgres)

3. Enable user code deployment:

Due to the type of sink we are using in our pipeline we need to make
some extra changes in order for the IMDG cluster (which Jet sits
on top of) be aware of the custom classes we have defined.

Please append following config lines to the `config/hazelcast.yaml`
file, at the end of the `hazelcast` block:

```yaml
  user-code-deployment:
    enabled: true
    provider-mode: LOCAL_AND_CACHED_CLASSES
```

Also add these config lines to the `config/hazelcast-client.yaml` file,
at the end of the `hazelcast-client` block:

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```yaml
  user-code-deployment:
    enabled: true
    jarPaths:
      - <path_to_project>/build/libs/cdc-tutorial-1.0-SNAPSHOT.jar
```

<!--Maven-->

```yaml
  user-code-deployment:
    enabled: true
    jarPaths:
      - <path_to_project>/target/cdc-tutorial-1.0-SNAPSHOT.jar
```

<!--END_DOCUSAURUS_CODE_TABS-->

Make sure to replace `<path_to_project>` with the absolute path to where
you created the project for this tutorial.

4. Start Jet:

```bash
bin/jet-start
```

5. When you see output like this, Jet is up:

```text
Members {size:1, ver:1} [
    Member [192.168.1.5]:5701 - e7c26f7c-df9e-4994-a41d-203a1c63480e this
]
```

## 8. Submit for Execution

Assuming our cluster is [running](#7-start-hazelcast-jet) and the
database [is up](#2-start-database), all we need to issue is
following command:

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
see these lines due to the `peek()` stages we've inserted):

```text
........
... Output to ordinal 0: key:{{"order_number":10002}}, value:{{"order_number":10002,"order_date":16817,"purchaser":1002,"quantity":2,"product_id":105,"__op":"c","__db":"inventory","__table":"orders","__ts_ms":1593681751174,"__deleted":"false"}} (eventTime=12:22:31.174)
... Output to ordinal 0: key:{{"order_number":10003}}, value:{{"order_number":10003,"order_date":16850,"purchaser":1002,"quantity":2,"product_id":106,"__op":"c","__db":"inventory","__table":"orders","__ts_ms":1593681751174,"__deleted":"false"}} (eventTime=12:22:31.174)
... Output to ordinal 0: key:{{"id":1003}}, value:{{"id":1003,"first_name":"Edward","last_name":"Walker","email":"ed@walker.com","__op":"c","__db":"inventory","__table":"customers","__ts_ms":1593681751161,"__deleted":"false"}} (eventTime=12:22:31.161)
........
```

## 9. Track Updates

Let's see how our cache looks like at this time. If we execute the
 `CacheRead` code [defined above](#5-define-jet-job), we'll get:

```text
Currently there are following customers in the cache:
    Customer: Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}, Orders: {10002=Order {orderNumber=10002, orderDate=Sun Jan 17 02:00:00 EET 2016, purchaser=1002, quantity=2, productId=105}, 10003=Order {orderNumber=10003, orderDate=Fri Feb 19 02:00:00 EET 2016, purchaser=1002, quantity=2, productId=106}}
    Customer: Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}, Orders: {10004=Order {orderNumber=10004, orderDate=Sun Feb 21 02:00:00 EET 2016, purchaser=1003, quantity=1, productId=107}}
    Customer: Customer {id=1004, firstName=Anne, lastName=Kretchmar, email=annek@noanswer.org}, Orders: {}
    Customer: Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}, Orders: {10001=Order {orderNumber=10001, orderDate=Sat Jan 16 02:00:00 EET 2016, purchaser=1001, quantity=1, productId=102}}
```

Let's do some updates in our database. Go to the database CLI
[we've started earlier](#3-start-command-line-client) and run
following commands:

```bash
INSERT INTO inventory.customers VALUES (1005, 'Jason', 'Bourne', 'jason@bourne.org');
DELETE FROM inventory.orders WHERE order_number=10002;
```

If we check the cache with `CacheRead` we get:

```text
Currently there are following customers in the cache:
    Customer: Customer {id=1005, firstName=Jason, lastName=Bourne, email=jason@bourne.org}, Orders: {}
    Customer: Customer {id=1002, firstName=George, lastName=Bailey, email=gbailey@foobar.com}, Orders: {10003=Order {orderNumber=10003, orderDate=Fri Feb 19 02:00:00 EET 2016, purchaser=1002, quantity=2, productId=106}}
    Customer: Customer {id=1003, firstName=Edward, lastName=Walker, email=ed@walker.com}, Orders: {10004=Order {orderNumber=10004, orderDate=Sun Feb 21 02:00:00 EET 2016, purchaser=1003, quantity=1, productId=107}}
    Customer: Customer {id=1004, firstName=Anne, lastName=Kretchmar, email=annek@noanswer.org}, Orders: {}
    Customer: Customer {id=1001, firstName=Sally, lastName=Thomas, email=sally.thomas@acme.com}, Orders: {10001=Order {orderNumber=10001, orderDate=Sat Jan 16 02:00:00 EET 2016, purchaser=1001, quantity=1, productId=102}}
```

## 10. Clean up

Let's clean-up after ourselves. First we cancel our Jet job:

```bash
<path_to_jet>/bin/jet cancel monitor
```

Then we shut down our Jet member/cluster:

```bash
<path_to_jet>/bin/jet-stop
```

<!--DOCUSAURUS_CODE_TABS-->

<!--MySQL-->

You can use Docker to stop all running containers:

```bash
docker stop mysqlterm mysql
```

<!--Postgres-->

You can use Docker to stop the running container (this will kill the
command-line client too, since it's running in the same container):

```bash
docker stop postgres
```

<!--END_DOCUSAURUS_CODE_TABS-->

Again, since we've used the `--rm` flag when starting the connectors,
Docker should remove them right after we stop them.
We can verify that all processes are stopped and removed with following
command:

```bash
docker ps -a
```
