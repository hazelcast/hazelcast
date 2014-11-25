

### Introduction to Aggregations API

We now look into the possible options of what can be achieved using the
Aggregations API. To work on some deeper examples, let's quickly have a look at the available classes and interfaces and
discuss their usage.

#### Supplier

The `com.hazelcast.mapreduce.aggregation.Supplier` provides filtering and data extraction to the aggregation operation.
This class already provides a few different static methods to achieve the most common cases. `Supplier.all()`
accepts all incoming values and does not apply any data extraction or transformation upon them before supplying them to
the aggregation function itself.

For filtering data sets, you have two different options by default. You can either supply a `com.hazelcast.query.Predicate`
if you want to filter on values and / or keys, or you can supply a `com.hazelcast.mapreduce.KeyPredicate` if you can decide directly on the data
key without the need to deserialize the value.

##### Basic Filtering

As mentioned above, all APIs are fully Java 8 and Lambda compatible. Let's have a look on how we can do basic filtering using
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

Using the standard Hazelcast `Predicate` interface, you can also filter based on the value of a data entry. In the following example, you can
only select values which are divisible by 4 without a remainder. 

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

##### Extracting and Transforming Data

As well as filtering, `Supplier` can also extract or transform data before providing it
to the aggregation operation itself. The following example shows how to transform an input value to a string.
 
```java
Supplier<String, Integer, String> supplier = Supplier.all(
    value -> Integer.toString(value)
);
```

You can see a Java 6 / 7 example in the [Aggregations Examples section](#aggregation-examples).

Apart from the fact we transformed the input value of type `int` (or Integer) to a string, we can see that the generic information
of the resulting `Supplier` has changed as well. This indicates that we now have an aggregation working on string values.

##### Chaining Multiple Filtering Rules

Another feature of `Supplier` is its ability to chain multiple filtering rules. Let's combine all of the
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

##### Implementing Based on Special Requirements

Last but not least, you might prefer to (or need to) implement your `Supplier` based on special
requirements. This is a very basic task. The `Supplier` abstract class has just one method.
<br></br>

![image](images/NoteSmall.jpg) ***NOTE:*** *Due to a limitation of the Java Lambda API, you cannot implement abstract classes using Lambdas. Instead it is
recommended that you create a standard named class.*
 
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
process. This can be used, as in the example above, to implement filter rules directly. Implementing filters using the
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
MapReduce API implementations like `Mapper`, `Combiner`, `Reducer`, and `Collator`. These implementations are normally unique to
the chosen `Aggregation`. This interface can also be implemented with your aggregation operations based on MapReduce calls. For
more information, refer to [Implementing Aggregations section](#implementing-aggregations).

The `com.hazelcast.mapreduce.aggregation.Aggregations` class provides a common predefined set of aggregations. This class
contains type safe aggregations of the following types:

 - Average (Integer, Long, Double, BigInteger, BigDecimal)
 - Sum (Integer, Long, Double, BigInteger, BigDecimal)
 - Min (Integer, Long, Double, BigInteger, BigDecimal, Comparable)
 - Max (Integer, Long, Double, BigInteger, BigDecimal, Comparable)
 - DistinctValues
 - Count

Those aggregations are similar to their counterparts on relational databases and can be equated to SQL statements as set out
below.
 
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

We used the `com.hazelcast.mapreduce.aggregation.PropertyExtractor` interface before when we had a look at the example
on how to use a `Supplier` to [transform a value to another type](#extracting-and-transforming-data). It can also be used to extract attributes from values.

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

In this example, we extract the value from the person's age attribute. The value type changes from Person to `Integer` which is reflected in the generics information to stay type safe.

`PropertyExtractor`s are meant to be used for any kind of transformation of data. You might even want to have multiple
transformation steps chained one after another.

#### Aggregation Configuration

As stated before, the easiest way to configure the resources used by the underlying MapReduce framework is to supply a `JobTracker`
to the aggregation call itself by passing it to either `IMap::aggregate` or `MultiMap::aggregate`.

There is another way to implicitly configure the underlying used `JobTracker`. If no specific `JobTracker` was
passed for the aggregation call, internally one will be created using the following naming specifications:

For `IMap` aggregation calls the naming specification is created as:

- `hz::aggregation-map-` and the concatenated name of the map.

For `MultiMap` it is very similar:

- `hz::aggregation-multimap-` and the concatenated name of the MultiMap.

Knowing that (the specification of the name), we can configure the `JobTracker` as expected 
(as described in the [Jobtracker section](#jobtracker)) using the naming spec we just learned. For more information on configuration of the 
`JobTracker`, please see the [JobTracker Configuration section](#jobtracker-configuration). 

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

