
## Using Wildcard

Hazelcast supports wildcard configuration for all distributed data structures that can be configured using `Config` (i.e. for all except `IAtomicLong`, `IAtomicReference`). Using an asterisk (\*) character in the name, different instances of maps, queues, topics, semaphores, etc. can be configured by a single configuration.

A single (only one) asterisk (\*) can be placed anywhere inside the configuration name.

For instance, a map named `com.hazelcast.test.mymap` can be configured using one of the following configurations.

```xml
<map name="com.hazelcast.test.*">
...
</map>
```
```xml
<map name="com.hazel*">
...
</map>
```
```xml
<map name="*.test.mymap">
...
</map>
```
```xml
<map name="com.*test.mymap">
...
</map>
```
Or a queue '`com.hazelcast.test.myqueue`':

```xml
<queue name="*hazelcast.test.myqueue">
...
</queue>
```
```xml
<queue name="com.hazelcast.*.myqueue">
...
</queue>
```

