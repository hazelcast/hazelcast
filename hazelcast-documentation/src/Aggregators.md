
## Aggregators

Based on the Hazelcast MapReduce framework, Aggregators are ready-to-use data aggregations. Those are typical operations like
sum up values, finding minimum or maximum values, calculating averages and other operations as you expect them to be available in
the relational database world.  

Aggregation operations are implemented, as mentioned above, on top of the MapReduce framework and therefore all operations can be
achieved using pure map-reduce calls but using the Aggregation feature is more convenient for a big set of standard operations.

### Aggregations Basics

This short section will quickly guide you through the basics of the Aggregations framework and some of the available classes.
We also will implement a first base example.

Aggregations are available on both types of map interfaces, `com.hazelcast.core.IMap` and `com.hazelcast.core.MultiMap`, using
the `aggregate` methods. Two possible overloads of that method are available to support customized resource management of the
underlying MapReduce framework by supplying a custom configured `com.hazelcast.mapreduce.JobTracker` instance. To find out how to
configure the MapReduce framework please see [JobTracker Configuration](#jobtracker-configuration) section. We will
later see another way to configure the automatically used MapReduce framework if no special `JobTracker` is supplied.

To make Aggregations most convenient to use and future proof, the API is already heavily optimized for Java 8 and future version
but still fully compatible to any Java version Hazelcast supports (Java 6 and Java 7). The biggest difference is how you have to
work with the Java generics; on Java 6 and 7 the generics resolving process is not as strong as on Java 8 and
upcoming Java versions. In addition, the whole Aggregations API has full Java 8 Project Lambda (or Closure, 
[JSR 335](https://jcp.org/en/jsr/detail?id=335)) support.

For illustration of the differences in Java 6 and 7 in comparison to Java 8, we  will have a quick look at both sourcecode
examples. After that, the documentation will focus on using Java 8 syntax to keep examples short and easy to understand but still
offers some hints what it looks like on Java 6 or 7 style.

The first basic example will produce the sum of some `int` values stored in a Hazelcast IMap. This is a very basic example not yet
using a lot of the functionality of the Aggregations framework but will already perfectly show the main difference.

```java
IMap<String, Integer> personAgeMapping = hazelcastInstance.getMap( "person-age" );
for ( int i = 0; i < 1000; i++ ) {
  String lastName = RandomUtil.randomLastName();
  int age = RandomUtil.randomAgeBetween( 20, 80 );
  personAgeMapping.put( lastName, Integer.valueOf( age ) );
}
```

Since our demo data are now prepared we can have a look at how to produce the sums in different Java versions.

#### Aggregations and Java 6 or Java 7

Since Java 6 and 7, as mentioned earlier, are not as strong on resolving generics as Java 8 we need to be a bit more verbose
on what we write or you might want to consider using raw types but breaking the type safety to ease this process.

For a short introduction on what the following lines mean have a quick look at the sourcecode comments. We will deeper dig into
the different options in a bit. 

```java
// No filter applied, select all entries
Supplier<String, Integer, Integer> supplier = Supplier.all();
// Choose the sum aggregation
Aggregation<String, Integer, Integer> aggregation = Aggregations.integerSum();
// Execute the aggregation
int sum = personAgeMapping.aggregate( supplier, aggregation );
```

#### Aggregations and Java 8

On Java 8 the Aggregations API looks much simpler since Java is now able to resolve the generic parameters for us. That means
the above lines of source will end up in one line on Java 8.

```
int sum = personAgeMapping.aggregate( Supplier.all(), Aggregations.integerSum() );
```

As you can see, this really looks stunning and easy to use.

#### Quick look at the MapReduce Framework

As mentioned before, the Aggregations implementation is based on the Hazelcast MapReduce framework and therefore you might find
overlaps of the different APIs and we have already seen one before. One overload of the `aggregate` method can be supplied with
a `JobTracker` which is part of the MapReduce framework.

If you are going to implement your own aggregations you also end up implementing them using a mixture of the Aggregations and
the MapReduce API. If you are looking forward to implement your own aggregation, e.g. to make the life of colleagues easier,
please read [Implementing Aggregations](#implementing-aggregations).

For the full MapReduce documentation please see [MapReduce](#mapreduce).


### Introduction to Aggregations API

Following the basic example, we now want to look into the real API, for the possible options and what can be achieved using the
Aggregations API. To work on some more and deeper examples, let's quickly have a look at the available classes and interfaces and
discuss their usage.

#### Supplier

The `com.hazelcast.mapreduce.aggregation.Supplier` is used to provide filtering and data extraction to the aggregation operation.
This class already provides a few different static methods to achieve most common cases. We already learned about `Supplier.all()`
which accepts all incoming values and does not apply any data extraction or transformation upon them before supplying them to
the aggregation function itself.

For filtering data sets, you have two different options by default. You can either supply a `com.hazelcast.query.Predicate`
if you want to filter on values and / or keys or a `com.hazelcast.mapreduce.KeyPredicate` if you can decide directly on the data
key without the need to deserialize the value.

As mentioned above, all APIs are fully Java 8 and Lambda compatible, so let's have a look on how we can do basic filtering using
those two options.

First, we have a look at a `KeyPredicate` and only accept people whose last name is "Jones".

```java
Supplier<...> supplier = Supplier.fromKeyPredicate(
    lastName -> "Jones".equalsIgnoreCase( lastName )
);
```

```java
class JonesKeyPredicate implements KeyPredicate<String> {
  public boolean evaluate( String key ) {
    return "Jones".equalsIgnoreCase( key );
  }
}
```

Using the standard Hazelcast `Predicate` interface, you can also filter based on the value of a data entry. For example, you can
only select values which are divisible without remainder by 4 using the following example. 

```java
Supplier<...> supplier = Supplier.fromPredicate(
    entry -> entry.getValue() % 4 == 0
);
```

```java
class DivisiblePredicate implements Predicate<String, Integer> {
  public boolean apply( Map.Entry<String, Integer> entry ) {
    return entry.getValue() % 4 == 0;
  }
}
```

Besides from the fact that a `Supplier` is used for filtering, it is also used to extract or transform data before supplying them
to the aggregation operation itself. We will now look into a short example on how to transform the input value to a string.
 
```java
Supplier<String, Integer, String> supplier = Supplier.all(
    value -> Integer.toString(value)
);
```

A Java 6 / 7 example will follow up below in the following section.

Apart from the fact we transformed the input value of type `int` (or Integer) to a string, we can see that the generic information
of the resulting `Supplier` has changed as well. This indicates that we now would have an aggregation working on string values.

Another bit to say about the `Supplier` is that it is possible to chain multiple filtering rules, so let's combine all of the
above examples into one rule set:

```java
Supplier<String, Integer, String> supplier =
    Supplier.fromKeyPredicate(
        lastName -> "Jones".equalsIgnoreCase( lastName ),
        Supplier.fromPredicate(
            entry -> entry.getValue() % 4 == 0,  
            Supplier.all( value -> Integer.toString(value) )
        )
    );
```

Last but not least, you might prefer (or end up in a necessary situation) implementing your `Supplier` based on special
requirements. But do not be afraid, this is a very basic task. The `Supplier` abstract class has just one method.
<br></br>

***NOTE:*** *Due to a limitation of the Java Lambda API you cannot implement abstract classes using Lambdas, so instead it is
recommended to create a standard named class instead.*
 
```java
class MyCustomSupplier extends Supplier<String, Integer, String> {
  public String apply( Map.Entry<String, Integer> entry ) {
    Integer value = entry.getValue();
    if (value == null) {
      return null;
    }
    return value % 4 == 0 ? String.valueOf( value ) : null;
  }
}
```

`Supplier`s are expected to return null from the `apply` method whenever the input value should not be mapped to the aggregation
process. This can be used, as seen above, to implement filter rules directly. Anyways, trying to implement filters using the
`KeyPredicate` and `Predicate` interfaces might be more convenient.

To use your own `Supplier`, just pass it to the aggregate method or use it in combination with other `Supplier`s.

```java
int sum = personAgeMapping.aggregate( new MyCustomSupplier(), Aggregations.count() );
```

```java
Supplier<String, Integer, String> supplier =
    Supplier.fromKeyPredicate(
        lastName -> "Jones".equalsIgnoreCase( lastName ),
        new MyCustomSupplier()
     );
int sum = personAgeMapping.aggregate( supplier, Aggregations.count() );
```

#### Aggregation and Aggregations

The `com.hazelcast.mapreduce.aggregation.Aggregation` interface defines the aggregation operation itself. It contains a set of
MapReduce API implementations like `Mapper`, `Combiner`, `Reducer`, `Collator`. These implementations are normally unique to
the chosen `Aggregation`. This interface can also be implemented with your aggregation operations based on map-reduce calls. To
find deeper information on this, please have a look at [Implementing Aggregations](#implementing-aggregations).

A common predefined set of aggregations are provided by the `com.hazelcast.mapreduce.aggregation.Aggregations` class. This class
contains typesafe aggregations of the following types:

 - Average (Integer, Long, Double, BigInteger, BigDecimal)
 - Sum (Integer, Long, Double, BigInteger, BigDecimal)
 - Min (Integer, Long, Double, BigInteger, BigDecimal, Comparable)
 - Max (Integer, Long, Double, BigInteger, BigDecimal, Comparable)
 - DistinctValues
 - Count

Those aggregations are a lot like their counterparts on relational databases. Said that we can mostly translate them to their
direct SQL like way of describing them:
 
##### Average #####

Calculates an average value based on all selected values.

```java
map.aggregate( Supplier.all( person -> person.getAge() ),
               Aggregations.integerAvg() );
```

```sql
SELECT AVG(person.age) FROM person;
```

##### Sum #####

Calculates a sum based on all selected values.

```java
map.aggregate( Supplier.all( person -> person.getAge() ),
               Aggregations.integerSum() );
```

```sql
SELECT SUM(person.age) FROM person;
```

##### Minimum (Min) #####

Finds the minimal value over all selected values.

```java
map.aggregate( Supplier.all( person -> person.getAge() ),
               Aggregations.integerMin() );
```

```sql
SELECT MIN(person.age) FROM person;
```

##### Maximum (Max) #####

Finds the maximal value over all selected values.

```java
map.aggregate( Supplier.all( person -> person.getAge() ),
               Aggregations.integerMax() );
```

```sql
SELECT MAX(person.age) FROM person;
```

##### Distinct Values #####   

Returns a collection of distinct values over the selected values

```java
map.aggregate( Supplier.all( person -> person.getAge() ),
               Aggregations.distinctValues() );
```

```sql
SELECT DISTINCT person.age FROM person;
```

##### Count #####    

Returns the element count over all selected values 

```java
map.aggregate( Supplier.all(), Aggregations.count() );
```

```sql
SELECT COUNT(*) FROM person;
```


#### PropertyExtractor

We have already used the `com.hazelcast.mapreduce.aggregation.PropertyExtractor` interface before when we had a look at the example
on how to use a `Supplier` to transform a value to another type. It can also be used to extract attributes from values.

```java
class Person {
  private String firstName;
  private String lastName;
  private int age;
  
  // getters and setters
}

PropertyExtractor<Person, Integer> propertyExtractor = (person) -> person.getAge();
```

```java
class AgeExtractor implements PropertyExtractor<Person, Integer> {
  public Integer extract( Person value ) {
    return value.getAge();
  }
}
```

In this example, we extract the value from the person's age attribute and so the value type changes from Person to `Integer` which
again is reflected in the generics information to stay typesafe.

`PropertyExtractor`s are meant to be used for any kind of transformation of data, you might even want to have multiple
transformation steps chained one after another.

#### Aggregation Configuration

As stated before, the easiest way to configure the resources used by the underlying MapReduce framework is to supply a `JobTracker`
to the aggregation call itself by passing it to either `IMap::aggregate` or `MultiMap::aggregate`.

There is also a second way on how to implicitly configure the underlying used `JobTracker`. If no specific `JobTracker` was
passed for the aggregation call, internally the one to be used will be created using a naming specification as the following:

For `IMap` aggregation calls the naming spec is created as:

- `hz::aggregation-map-` and concatenated the name of the map

For `MultiMap` it is very similar:

- `hz::aggregation-multimap-` and concatenated the name of the multimap

Knowing that we can configure the `JobTracker` in the Hazelcast configuration file as expected and as name we use the previously
built name due to the specification. For more information on configuration of the `JobTracker` please see
[JobTracker Configuration](#jobtracker-configuration). 

To finish this section, let's have a quick example for the above naming specs:

```java
IMap<String, Integer> map = hazelcastInstance.getMap( "mymap" );

// The internal JobTracker name resolves to 'hz::aggregation-map-mymap' 
map.aggregate( ... );
```

```java
MultiMap<String, Integer> multimap = hazelcastInstance.getMultiMap( "mymultimap" );

// The internal JobTracker name resolves to 'hz::aggregation-multimap-mymultimap' 
multimap.aggregate( ... );
```

### Aggregations Examples

For the final example, imagine you are working for an international company and you have an employee database stored in Hazelcast
`IMap` with all employees worldwide and a `MultiMap` for assigning employees to their certain locations or offices. In addition,
there is another `IMap` which holds the salary per employee.

Let's have a look at our data model:

```java
class Employee implements Serializable {
  private String firstName;
  private String lastName;
  private String companyName;
  private String address;
  private String city;
  private String county;
  private String state;
  private int zip;
  private String phone1;
  private String phone2;
  private String email;
  private String web;

  // getters and setters
}

class SalaryMonth implements Serializable {
  private Month month;
  private int salary;
  
  // getters and setters
}

class SalaryYear implements Serializable {
  private String email;
  private int year;
  private List<SalaryMonth> months;
  
  // getters and setters
  
  public int getAnnualSalary() {
    int sum = 0;
    for ( SalaryMonth salaryMonth : getMonths() ) {
      sum += salaryMonth.getSalary();
    }
    return sum;
  }
}
```

On the other hand, our data structures, the two `IMap`s and the `MultiMap`, are defined as the following:

```java
IMap<String, Employee> employees = hz.getMap( "employees" );
IMap<String, SalaryYear> salaries = hz.getMap( "salaries" );
MultiMap<String, String> officeAssignment = hz.getMultiMap( "office-employee" );
```

So far, we know all important information to work out some example aggregations. We will look into some deeper implementation
details and how we can work around some current limitations that will be eliminated in future versions of the API.

So let's start with an already seen, very basic example. We want to know the average salary of all of our employees. To do this,
we need a `PropertyExtractor` and the average aggregation for type `Integer`.

```java
IMap<String, SalaryYear> salaries = hazelcastInstance.getMap( "salaries" );
PropertyExtractor<SalaryYear, Integer> extractor =
    (salaryYear) -> salaryYear.getAnnualSalary();
int avgSalary = salaries.aggregate( Supplier.all( extractor ),
                                    Aggregations.integerAvg() );
```

That's it. Internally, we created a map-reduce task based on the predefined aggregation and fire it up immediately. Currently, all
aggregation calls are blocking operations, so it is not yet possible to execute the aggregation in a reactive way (using
`com.hazelcast.core.ICompletableFuture`) but this will be part of one of the upcoming versions.

The following example is already a bit more complex, so we only want to have our US based employees selected into the average
salary calculation, so we need to execute some kind of a join operation between the employees and salaries maps.

```java
class USEmployeeFilter implements KeyPredicate<String>, HazelcastInstanceAware {
  private transient HazelcastInstance hazelcastInstance;
  
  public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
    this.hazelcastInstance = hazelcastInstance;
  }
  
  public boolean evaluate( String email ) {
    IMap<String, Employee> employees = hazelcastInstance.getMap( "employees" );
    Employee employee = employees.get( email );
    return "US".equals( employee.getCountry() );
  }
}
```

Using the `HazelcastInstanceAware` interface, we get the current instance of Hazelcast injected into our filter and can perform data
joins on other data structures of the cluster. We now only select employees that work as part of our US offices into the
aggregation.

```java
IMap<String, SalaryYear> salaries = hazelcastInstance.getMap( "salaries" );
PropertyExtractor<SalaryYear, Integer> extractor =
    (salaryYear) -> salaryYear.getAnnualSalary();
int avgSalary = salaries.aggregate( Supplier.fromKeyPredicate(
                                        new USEmployeeFilter(), extractor
                                    ), Aggregations.integerAvg() );
```

As mentioned before, this example already was a bit more sophisticated. For our next example, we will do some grouping based on
the different worldwide offices. Currently, a group aggregator is not yet available, that means we need a small workaround to
achieve this goal. In later versions of the Aggregations API this will not be sufficient anymore since it will be available out of
the box by a much more convenient way.

So again, let's start with our filter. This time we want to filter based on an office name and we again need to do some data joins
to achieve this kind of filtering. 

**A short tip:** to minimize the data transmission on the aggregation we can use
[Data Affinity](#data-affinity) rules to influence the partitioning of data. Be aware that this is an expert feature of Hazelcast.

```java
class OfficeEmployeeFilter implements KeyPredicate<String>, HazelcastInstanceAware {
  private transient HazelcastInstance hazelcastInstance;
  private String office;
  
  // Deserialization Constructor
  public OfficeEmployeeFilter() {
  } 
  
  public OfficeEmployeeFilter( String office ) {
    this.office = office;
  }
  
  public void setHazelcastInstance( HazelcastInstance hazelcastInstance ) {
    this.hazelcastInstance = hazelcastInstance;
  }
  
  public boolean evaluate( String email ) {
    MultiMap<String, String> officeAssignment = hazelcastInstance
        .getMultiMap( "office-employee" );

    return officeAssignment.containsEntry( office, email );    
  }
}
```

Now, we can execute our aggregations. As mentioned, we currently need to do the grouping on our own by executing multiple
aggregations in a row but that will go away soon.

```java
Map<String, Integer> avgSalariesPerOffice = new HashMap<String, Integer>();

IMap<String, SalaryYear> salaries = hazelcastInstance.getMap( "salaries" );
MultiMap<String, String> officeAssignment =
    hazelcastInstance.getMultiMap( "office-employee" );

PropertyExtractor<SalaryYear, Integer> extractor =
    (salaryYear) -> salaryYear.getAnnualSalary();

for ( String office : officeAssignment.keySet() ) {
  OfficeEmployeeFilter filter = new OfficeEmployeeFilter( office );
  int avgSalary = salaries.aggregate( Supplier.fromKeyPredicate( filter, extractor ),
                                      Aggregations.integerAvg() );
                                      
  avgSalariesPerOffice.put( office, avgSalary );
}
```

After the previous example, we want to fade out from this section by executing one final, easy but nice aggregation. We
just want to know how many employees we currently have on a worldwide basis. Before reading the next lines of sourcecode, you
can try to do it on your own to see if you understood the way of executing aggregations.

As said, this is again a very basic example but it is the perfect closing point for this section:

```java
IMap<String, Employee> employees = hazelcastInstance.getMap( "employees" );
int count = employees.size();
```

Ok, after that quick joke, we look at the real two code lines:

```java
IMap<String, Employee> employees = hazelcastInstance.getMap( "employees" );
int count = employees.aggregate( Supplier.all(), Aggregations.count() );
```

We now have a good overview of how to use aggregations in real life situations. If you want to do your colleagues a favor you
might want to end up writing your own additional set of aggregations. Then please read on the next section, if not just stop
here.

### Implementing Aggregations

This section explains how to implement your own aggregations for convenient reasons in your own application. It
is meant to be an advanced users section so if you do not intend to implement your own aggregation, you might want to
stop reading here and probably come back at a later point in time when there is the need to know how to implement your own
aggregation.

The main interface for making your own aggregation is `com.hazelcast.mapreduce.aggregation.Aggregation`. It consists of four
methods that can be explained very briefly.
 
```java
interface Aggregation<Key, Supplied, Result> {
  Mapper getMapper(Supplier<Key, ?, Supplied> supplier);
  CombinerFactory getCombinerFactory();
  ReducerFactory getReducerFactory();
  Collator<Map.Entry, Result> getCollator();
}
```
 
As we can see, an `Aggregation` implementation is nothing more than defining a map-reduce task with a small difference. The `Mapper`
is always expected to work on a `Supplier` that filters and / or transforms the mapped input value to some output value.

Whereas, `getMapper` and `getReducerFactory` are expected to return non-null values, `getCombinerFactory` and `getCollator` are
optional operations and do not need to be implemented. If you want to implement these, it heavily depends on your use case you want
to achieve.

For more information on how you implement mappers, combiners, reducer and collators you should have a look at the
[MapReduce](#mapreduce) section, since it is out of the scope of this chapter to explain it.

For best speed and traffic usage, as mentioned in the map-reduce documentation, you should add a `Combiner` to your aggregation
whenever it is possible to do some kind of pre-reduction step.

Your implementation also should use DataSerializable or IdentifiedDataSerializable for best compatibility and speed / stream-size
reasons.

<br></br>