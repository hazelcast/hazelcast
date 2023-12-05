This directory contains resources for testing user code deployment use cases.

Contents:

- `ChildClass.class`, `ParentClass.class`: hierarchy of classes where `ChildClass extends ParentClass`.
- `IncrementingExternalEntryProcessor.class`: an `EntryProcessor` for `<Integer, Integer>` entries that increments value by 1 - renamed to avoid `IncrementingEntryProcessor` already on the classpath.
- `IncrementingJavaxEntryProcessor.class`: a `javax.cache.processor.EntryProcessor<K, V, T>` for `<Integer, Integer>` entries that increments value by 1.
- `IncrementingValueExtractor`: a `ValueExtractor` that increments value by 1.
- `IncrementingMapInterceptor.class`: an `MapInterceptor` that increments value by 1.
- `ShadedClasses.jar`: contains a class `com.hazelcast.core.HazelcastInstance` that defines a `main` method.
- `IncrementingEntryProcessor.jar`: contains `IncrementingEntryProcessor` class.
- `ChildParent.jar`: contains `ChildClass` and `ParentClass` as described above.
- `EntryProcessorWithAnonymousAndInner.jar`: contains class `EntryProcessorWithAnonymousAndInner`, to exercise loading classes with anonymous and named inner classes.
- `LowerCaseValueEntryProcessor`, `UpperCaseValueEntryProcessor`: `EntryProcessor` who adjust the case of the value, with deliberately overlapping class names.
- `AcceptAllIFunction`: a simple `IFunction` that always returns `true`.
- `DerbyUpperCaseStringMapLoader`: connects to an embedded Derby database instance and returns the result of an upper-casing SQL query.
- `H2WitHDataSourceBuilderVerionMapLoader`, `H2WithDriverManagerBuildVersionMapLoader`: connects to an embedded H2 database and returns the results of an SQL query for the databases' version.
- `IdentityProjection`: a `Projection` that returns the input.
- `CustomCachePartitionLostListener`: a simple CachePartitionLostListener implementation that pushes notifications to an ISet
- `CustomMapPartitionLostListener`: a simple MapPartitionLostListener implementation that pushes notifications to an ISet
- `CustomSplitBrainProtectionListener`: a simple SplitBrainProtectionListener implementation that pushes notifications to an ISet

Note: unless package is explicitly specified, all classes described above reside in package `usercodedeployment`.

To generate a new `.class` from from an existing `.java` file, run something like:

```shell
mvn dependency:get -DgroupId=com.hazelcast -DartifactId=hazelcast -Dversion=5.3.2 --quiet;
mvn dependency:get -DgroupId=javax.cache -DartifactId=cache-api -Dversion=1.1.1 --quiet;

javac --release 11 *.java -cp "$HOME/.m2/repository/com/hazelcast/hazelcast/5.3.2/*:$HOME/.m2/repository/javax/cache/cache-api/1.1.1/*";
```

And to  compile those `.class`' into a `.jar`:

```shell
jar -cf usercodedeployment/UCDTest.jar \
usercodedeployment/AcceptAllIFunction.class \
usercodedeployment/IdentityProjection.class \
usercodedeployment/IncrementingExternalEntryProcessor.class \
usercodedeployment/IncrementingJavaxEntryProcessor.class \
usercodedeployment/IncrementingMapInterceptor.class \
usercodedeployment/IncrementingOffloadableEntryProcessor.class \
usercodedeployment/IncrementingValueExtractor.class \
usercodedeployment/KeyBecomesValueEntryLoader.class \
usercodedeployment/KeyBecomesValueEntryStore.class \
usercodedeployment/KeyBecomesValueMapLoader.class \
usercodedeployment/KeyBecomesValueMapStore.class \
usercodedeployment/KeyBecomesValueQueueStore.class \
usercodedeployment/LargeSequenceRingBufferStore.class \
usercodedeployment/LargeSequenceRingBufferStoreFactory.class \
usercodedeployment/MyCallable.class \
usercodedeployment/MyEntryListener.class \
usercodedeployment/MyItemListener.class \
usercodedeployment/MyMessageListener.class \
usercodedeployment/NoOpCachePartitionLostListener.class \
usercodedeployment/ObservableListener.class \
usercodedeployment/TruePagingPredicate.class \
usercodedeployment/TruePartition1PartitionPredicate.class \
usercodedeployment/TruePredicate.class \
usercodedeployment/CustomCachePartitionLostListener.class \
usercodedeployment/CustomMapPartitionLostListener.class \
usercodedeployment/CustomSplitBrainProtectionListener.class
```