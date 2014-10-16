
## Aggregators

Based on the Hazelcast MapReduce framework, Aggregators are ready-to-use data aggregations. These are typical operations like
sum up values, finding minimum or maximum values, calculating averages and other operations that you would expect to be available 
in the relational database world.  

Aggregation operations are implemented, as mentioned above, on top of the MapReduce framework and therefore all operations can be
achieved using pure map-reduce calls but using the Aggregation feature is more convenient for a big set of standard operations.

### Aggregations Basics

This short section will quickly guide you through the basics of the Aggregations framework and some of the available classes.
We also will implement a first base example.

Aggregations are available on both types of map interfaces, `com.hazelcast.core.IMap` and `com.hazelcast
.core.MultiMap`, using
the `aggregate` methods. Two overloaded methods are available to customize resource management of the
underlying MapReduce framework by supplying a custom configured `com.hazelcast.mapreduce.JobTracker` instance. To find out how to
configure the MapReduce framework please see [JobTracker Configuration](#jobtracker-configuration) section. We will
later see another way to configure the automatically used MapReduce framework if no special `JobTracker` is supplied.

To make Aggregations more convenient to use and future proof, the API is already heavily optimized for Java 8 and future version
but still fully compatible to any Java version Hazelcast supports (Java 6 and Java 7). The biggest difference is how you have to
work with the Java generics; on Java 6 and 7 the generics resolving process is not as strong as on Java 8 and
upcoming Java versions. In addition, the whole Aggregations API has full Java 8 Project Lambda (or Closure, 
[JSR 335](https://jcp.org/en/jsr/detail?id=335)) support.

For illustration of the differences in Java 6 and 7 in comparison to Java 8, we  will have a quick look at both source code
examples. After that, the documentation will focus on using Java 8 syntax to keep examples short and easy to understand but still
offer some hints as to what it looks like in Java 6 or 7.

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

With our demo data prepared we can have a look at how to produce the sums in different Java versions.

#### Aggregations and Java 6 or Java 7

Since Java 6 and 7, as mentioned earlier, are not as strong on resolving generics as Java 8 we need to be a bit more verbose
with what we write or you might want to consider using raw types but breaking the type safety to ease this process.

For a short introduction on what the following lines mean have a quick look at the source code comments. We will dig deeper into
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


