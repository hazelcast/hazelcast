
### Implementing Aggregations

This section explains how to implement your own aggregations in your own application. It
is meant to be an advanced section, so if you do not intend to implement your own aggregation, you might want to
stop reading here and come back later when you need to know how to implement your own
aggregation.

The main interface for making your own aggregation is `com.hazelcast.mapreduce.aggregation.Aggregation`. It consists of four
methods.
 
```java
interface Aggregation<Key, Supplied, Result> {
  Mapper getMapper(Supplier<Key, ?, Supplied> supplier);
  CombinerFactory getCombinerFactory();
  ReducerFactory getReducerFactory();
  Collator<Map.Entry, Result> getCollator();
}
```
 
An `Aggregation` implementation is just defining a MapReduce task with a small difference: the `Mapper`
is always expected to work on a `Supplier` that filters and / or transforms the mapped input value to some output value.

`getMapper` and `getReducerFactory` are expected to return non-null values. `getCombinerFactory` and `getCollator` are
optional operations and do not need to be implemented. If you can decide to implement them depending on the use case you want
to achieve.

For more information on how you implement mappers, combiners, reducers, and collators, refer to the
[MapReduce section](#mapreduce) section.

For best speed and traffic usage, as mentioned in the [MapReduce section](#mapreduce), you should add a `Combiner` to your aggregation
whenever it is possible to do some kind of pre-reduction step.

Your implementation also should use `DataSerializable` or `IdentifiedDataSerializable` for best compatibility and speed / stream-size
reasons.

<br></br>