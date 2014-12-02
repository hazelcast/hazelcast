
## Aggregators

Based on the Hazelcast MapReduce framework, Aggregators are ready-to-use data aggregations. These are typical operations like
sum up values, finding minimum or maximum values, calculating averages, and other operations that you would expect 
in the relational database world.  

Aggregation operations are implemented, as mentioned above, on top of the MapReduce framework and all operations can be
achieved using pure MapReduce calls. However, using the Aggregation feature is more convenient for a big set of standard operations.

### Aggregations Basics

This section will quickly guide you through the basics of the Aggregations framework and some of its available classes.
We also will implement a first base example.

Aggregations are available on both types of map interfaces, `com.hazelcast.core.IMap` and `com.hazelcast
.core.MultiMap`, using
the `aggregate` methods. Two overloaded methods are available that customize resource management of the
underlying MapReduce framework by supplying a custom configured `com.hazelcast.mapreduce.JobTracker` instance. To find out how to
configure the MapReduce framework, please see the [JobTracker Configuration section](#jobtracker-configuration). We will
later see another way to configure the automatically used MapReduce framework if no special `JobTracker` is supplied.

To make Aggregations more convenient to use and future proof, the API is heavily optimized for Java 8 and future versions.
The API is still fully compatible with any Java version Hazelcast supports (Java 6 and Java 7). The biggest difference is how you
work with the Java generics: on Java 6 and 7, the process to resolve generics is not as strong as on Java 8 and
upcoming Java versions. In addition, the whole Aggregations API has full Java 8 Project Lambda (or Closure, 
[JSR 335](https://jcp.org/en/jsr/detail?id=335)) support.

For illustration of the differences in Java 6 and 7 in comparison to Java 8, we will have a quick look at code
examples for both. After that, we will focus on using Java 8 syntax to keep examples short and easy to understand, and we will see some hints as to what the code looks like in Java 6 or 7.

The first example will produce the sum of some `int` values stored in a Hazelcast IMap. This example does not use much of the functionality of the Aggregations framework, but it will show the main difference.

```java
IMap<String, Integer> personAgeMapping = hazelcastInstance.getMap( "person-age" );
for ( int i = 0; i < 1000; i++ ) {
  String lastName = RandomUtil.randomLastName();
  int age = RandomUtil.randomAgeBetween( 20, 80 );
  personAgeMapping.put( lastName, Integer.valueOf( age ) );
}
```

With our demo data prepared, we can see how to produce the sums in different Java versions.

#### Aggregations and Java 6 or Java 7

Since Java 6 and 7 are not as strong on resolving generics as Java 8, you need to be a bit more verbose
with the code you write. You might also consider using raw types, but breaking the type safety to ease this process.

For a short introduction on what the following code example means, look at the source code comments. We will later dig deeper into
the different options. 

```java
// No filter applied, select all entries
Supplier<String, Integer, Integer> supplier = Supplier.all();
// Choose the sum aggregation
Aggregation<String, Integer, Integer> aggregation = Aggregations.integerSum();
// Execute the aggregation
int sum = personAgeMapping.aggregate( supplier, aggregation );
```

#### Aggregations and Java 8

With Java 8, the Aggregations API looks simpler because Java 8 can resolve the generic parameters for us. That means
the above lines of Java 6/7 example code will end up in just one easy line on Java 8.

```
int sum = personAgeMapping.aggregate( Supplier.all(), Aggregations.integerSum() );
```


#### Quick look at the MapReduce Framework

As mentioned before, the Aggregations implementation is based on the Hazelcast MapReduce framework and therefore you might find
overlaps in their APIs. One overload of the `aggregate` method can be supplied with
a `JobTracker` which is part of the MapReduce framework.

If you implement your own aggregations, you will use a mixture of the Aggregations and
the MapReduce API. If you will implement your own aggregation, e.g. to make the life of colleagues easier,
please read the [Implementing Aggregations section](#implementing-aggregations).

For the full MapReduce documentation please see the [MapReduce section](#mapreduce).


