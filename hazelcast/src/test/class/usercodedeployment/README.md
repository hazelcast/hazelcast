This directory contains resources for testing user code deployment use cases.

Contents:

- `ShadedClasses.jar`: contains a class `com.hazelcast.core.HazelcastInstance` that defines a `main` method.
- `IncrementingEntryProcessor.jar`: contains `IncrementingEntryProcessor` class.
- `ChildParent.jar`: contains `ChildClass` and `ParentClass` as described above.
- `EntryProcessorWithAnonymousAndInner.jar`: contains class `EntryProcessorWithAnonymousAndInner`, to exercise loading classes with anonymous and named inner classes.

Note: unless package is explicitly specified, all classes described above reside in package `usercodedeployment`.

To generate a new `.class` from an existing `.java` file, run something like:

```shell
mvn dependency:get -DgroupId=com.hazelcast -DartifactId=hazelcast -Dversion=5.3.2 --quiet;
mvn dependency:get -DgroupId=javax.cache -DartifactId=cache-api -Dversion=1.1.1 --quiet;

javac --release 11 *.java -cp "$HOME/.m2/repository/com/hazelcast/hazelcast/5.3.2/*:$HOME/.m2/repository/javax/cache/cache-api/1.1.1/*";
```