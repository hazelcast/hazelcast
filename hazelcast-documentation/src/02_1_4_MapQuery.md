

### Query

Hazelcast partitions your data and spreads across cluster of servers. You can surely iterate over the map entries and look for certain entries you are interested in but this is not very efficient as you will have to bring entire entry set and iterate locally. Instead, Hazelcast allows you to run distributed queries on your distributed map.

Let's say you have a "employee" map containing values of `Employee` objects:

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

**Distributed SQL Query**

`SqlPredicate` takes regular SQL where clause. Here is an example:

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

**Criteria API**

If SQL is not enough or programmable queries are preferred then JPA criteria like API can be used. Here is an example:

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