


## Query


Hazelcast partitions your data and spreads across cluster of servers. You can surely iterate over the map entries and look for certain entries you are interested in but this is not very efficient as you will have to bring entire entry set and iterate locally. Instead, Hazelcast allows you to run distributed queries on your distributed map.

Assume that you have an "employee" map containing values of `Employee` objects:

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

Now you are looking for the employees who are active and with age less than 30. Hazelcast allows you to find these entries in two different ways:

### Distributed SQL Query

`SqlPredicate` takes regular SQL `where` clause. Here is an example:

```java
import com.hazelcast.core.IMap;
import com.hazelcast.query.SqlPredicate;

Config cfg = new Config();
HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
IMap map = hz.getMap("employee");

Set<Employee> employees = (Set<Employee>) map.values(new SqlPredicate("active AND age < 30"));
```

Supported SQL syntax:

-   AND/OR

    -   `<expression> AND <expression> AND <expression>... `

        -   `active AND age>30`

        -   `active=false OR age = 45 OR name = 'Joe' `

        -   `active AND (age >20 OR salary < 60000) `

-   `=, !=, <, <=, >, >=`

    -   `<expression> = value`

        -   `age <= 30`

        -   `name ="Joe"`

        -   `salary != 50000`

-   BETWEEN

    -   `<attribute> [NOT] BETWEEN <value1> AND <value2>`

        -   `age BETWEEN 20 AND 33 (same as age >=20  AND age<=33)`

        -   `age NOT BETWEEN 30 AND 40 (same as age <30 OR age>40)`

-   LIKE

    -   `<attribute> [NOT] LIKE 'expression'`

        `%` (percentage sign) is placeholder for many characters, `_` (underscore) is placeholder for only one character.

        -   `name LIKE 'Jo%'` (true for 'Joe', 'Josh', 'Joseph' etc.)

        -   `name LIKE 'Jo_'` (true for 'Joe'; false for 'Josh')

        -   `name NOT LIKE 'Jo_'` (true for 'Josh'; false for 'Joe')

        -   `name LIKE 'J_s%'` (true for 'Josh', 'Joseph'; false 'John', 'Joe')

-   IN

    -   `<attribute> [NOT] IN (val1, val2,...)`

        -   `age IN (20, 30, 40)`

        -   `age NOT IN (60, 70)`

Examples:

-   `active AND (salary >= 50000 OR (age NOT BETWEEN 20 AND 30)) `

-   `age IN (20, 30, 40) AND salary BETWEEN (50000, 80000)`

### Criteria API

If SQL is not enough or programmable queries are preferred, then JPA criteria like API can be used. Here is an example:

```java
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.EntryObject;
import com.hazelcast.config.Config;


Config cfg = new Config();
HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
IMap map = hz.getMap("employee");

EntryObject e = new PredicateBuilder().getEntryObject();
Predicate predicate = e.is("active").and(e.get("age").lessThan(30));

Set<Employee> employees = (Set<Employee>) map.values(predicate);
```

### Paging Predicate (Order & Limit)

Hazelcast provides paging for defined predicates. For this purpose, `PagingPredicate` class has been developed. You may want to get collection of keys, values or entries page by page, by filtering them with predicates and giving the size of pages. Also, you can sort the entries by specifying comparators.

Below is a sample code where the `greaterEqual` predicate is used to get values from "students" map. This predicate puts a filter such that the objects with value of "age" is greater than or equal to 18 will be retrieved. Then, a `pagingPredicate` is constructed in which the page size is 5. So, there will be 5 objects in each page. 

The first time the values are called will constitute the first page. You can get the subsequent pages by using the `nextPage()` method of `PagingPredicate` and querying the map again with updated `PagingPredicate`..


```
final IMap<Integer, Student> map = instance.getMap("students");
       final Predicate greaterEqual = Predicates.greaterEqual("age", 18);
       final PagingPredicate pagingPredicate = new PagingPredicate(greaterEqual, 5);
       Collection<Student> values = map.values(pagingPredicate); //First Page
       ...
       
       pagingPredicate.nextPage();
       values = map.values(pagingPredicate); //Second Page
       ...
```

Paging Predicate is not supported in Transactional Context.

***Note***: *Please refer to [here](http://hazelcast.org/docs/latest/javadoc/com/hazelcast/query/Predicates.html) for all predicates.*



### Indexing

Hazelcast distributed queries will run on each member in parallel and only results will return the conn. When a query runs on a member, Hazelcast will iterate through the entire owned entries and find the matching ones. This can be made faster by indexing the mostly queried fields. Just like you would do for your database. Of course, indexing will add overhead for each `write` operation but queries will be a lot faster. If you are querying your map a lot, make sure to add indexes for most frequently queried fields. So, if your `active and age < 30 ` query, for example, is used a lot, make sure you add index for `active` and `age` fields. Here is how:

```java
Config cfg = new Config();
HazelcastInstance instance = Hazelcast.newHazelcastInstance(cfg);
IMap imap = instance.getMap("employees");
imap.addIndex("age", true);        // ordered, since we have ranged queries for this field
imap.addIndex("active", false);    // not ordered, because boolean field cannot have range
```

`IMap.addIndex(fieldName, ordered)` is used for adding index. For each indexed field, if you have ranged queries such as `age>30`, `age BETWEEN 40 AND 60`, then `ordered` parameter should be`true`. Otherwise, set it to`false`.

Also, you can define `IMap` indexes in configuration, a sample of which is shown below.


	```xml
	<map name="default">
	    ...
	    <indexes>
	        <index ordered="false">name</index>
	        <index ordered="true">age</index>
	    </indexes>
	</map>```


This sample in programmatic configuration looks like below.



	```java
	mapConfig.addMapIndexConfig(new MapIndexConfig("name", false));
	mapConfig.addMapIndexConfig(new MapIndexConfig("age", true));```


And, the following is the Spring declarative configuration for the same sample.
 


	```xml
	<hz:map name="default">
	    <hz:indexes>
	        <hz:index attribute="name"/>
	        <hz:index attribute="age" ordered="true"/>
	    </hz:indexes>
	</hz:map>
```
