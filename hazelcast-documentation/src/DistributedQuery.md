

# Distributed Query

## Query Overview


Hazelcast partitions your data and spreads across cluster of servers. You can surely iterate over the map entries and look for certain entries (specified by predicates) you are interested in but this is not very efficient as you will have to bring entire entry set and iterate locally. Instead, Hazelcast allows you to run distributed queries on your distributed map.


#### How It Works

-	Requested predicate is sent to each member in the cluster.
-	Each member looks at its own local entries and filters them according to the predicate. At this stage, key/value pairs of the entries are deserialized and then passed to the predicate.
-	Then the predicate requester merges all the results come from each member into a single set.

If you add new members to the cluster, partition count for each member is reduced and hence the time spent by each member on iterating its entries is reduced, too. So, the above querying approach is highly scalable. Another reason for being highly scalable is that, it is the pool of partition threads that evaluates the entries concurrently in each member. And, as you can guess, the network traffic is also reduced since only filtered data is sent to the requester.

Hazelcast offers below APIs for distributed query purposes:

- Criteria API
- Distributed SQL Query
<br></br>

Assume that you have an "employee" map containing values of `Employee` objects, as coded below.

```java
import java.io.Serializable;

public class Employee implements Serializable {
private String name;
private int age;
private boolean active;
private double salary;

public Employee(String name, int age, boolean live, double price) {
    this.name = name;
    this.age = age;
    this.active = live;
    this.salary = price;
}

public Employee() {
}

public String getName() {
    return name;
}

public int getAge() {
    return age;
}

public double getSalary() {
    return salary;
}

public boolean isActive() {
    return active;
}
}
```

Now, let's look for the employees who are active and with age less than 30 using the aforementioned APIs (Criteria API and Distributed SQL Query). Below subsections describes each query mechanism for this sample.

![image](images/NoteSmall.jpg)***NOTE:*** *When using Portable objects, if one field of an object exists on one node but does not exist on another one, Hazelcast does not throw an unknown field exception.
Instead, Hazelcast treats that predicate, which tries to perform a query on an unknown field, as an always false predicate.*


