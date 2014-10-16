
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