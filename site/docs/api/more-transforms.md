---
title: More Transforms
description: Pipeline transforms that don't neatly fit into the Stateless or Stateful category
---

## rebalance

Hazelcast Jet prefers not to send the data around the computing cluster.
If your data source retrieves some part of the data stream on member A
and you apply stateless mapping on it, this processing will happen on
member A. Jet will send the data only when needed to achieve
correctness, for example in the case of non-parallelizable operations
like `mapStateful`. Such transforms must be performed on a single
member, using a single Jet processor and all the data received on any
other member must be sent to the processing one.

The above policy results in the best throughput in most cases. However,
in some cases there is an inherent imbalance among cluster members in
terms of how much data they get from a data source. The most important
example are non-parallelized sources, where a single processor on a
single Jet member receives all the data. In such a case you can apply
the `rebalance` operator, which orders Jet to send the data out to other
members where normally it wouldn't choose to.

Rebalancing is best explained on the DAG level. Each pipeline stage
corresponds to a vertex in the DAG, and the logic attached to the edge
between them decides for each data item which processor to send it to.
Some processors are on the same machine and others are on remote
machines. By default, Jet considers only the processors on the local
machine as candidates, using a round-robin scheme to decide on the
target. When you apply rebalancing, Jet simply extends the candidate set
to all the processors, including those on the other machines, but keeps
using the same round-robin scheme. The order of the round-robin is such
that the target cluster member changes every time, maximizing the
fairness of the distribution across members.

Round-robin routing takes into account backpressure: if a given
processor is overloaded and its input queue is full, Jet tries the next
one. If during rebalancing the network becomes a bottleneck,
backpressure will automatically divert more traffic to the local
processors.

You can also apply a less flexible kind of rebalancing, which will
enforce sending to other members even when the local ones are more
available: `rebalance(keyFn)`. It uses the `keyFn` you supply as a
partitioning function. In this case every item is tied to one definite
choice of the destination processor and backpressure cannot override it.
If some processor must apply backpressure, Jet doesn't try to send the
data item to another available processor and instead propagates the
backpressure to the upstream vertex. This kind of rebalancing may result
in a better balanced CPU load across the cluster, but has the potential
for less overall throughput.

## peek

`stage.peek()` is an identity transform that adds diagnostic
side-effects: it logs every item it receives. Since the logging happens
on the machine that is processing the item, this transform is primarily
intended to be used during development.

## customTransform

All the data processing in a Jet pipeline happens inside the
implementations of the `Processor` interface, a central part of the Jet
Core API. With `stage.customTransform` you can provide your own
processor implementation that presumably does something that no
out-of-the-box processor offered through the Pipeline API can. If you
get involved with this transform, make sure you are familiar with the
internals of Hazelcast Jet, as exposed through the Core
[DAG](/docs/architecture/distributed-computing) API.

## JSON

JSON is very frequent data exchange format. To transform the data
from/to JSON format you can use `JsonUtil` utility class without adding
an extra dependency to the classpath. The utility class uses the
lightweight `jackson-jr` JSON library under the hood.

For example, you can convert JSON formatted string to a bean. You need
to have your bean fields as `public` or have public getters/setters and
a no-argument(default) constructor.

```json
{
  "name": "Jet",
  "age": 4
}
```

```java
public class Person {
    public String name;
    public int age;

    public Person() {
    }
}
```

```java
BatchStage<Person> persons = stage.map(jsonString -> JsonUtil.beanFrom(jsonString, Person.class));
```

If you don't have a bean class, you can use `mapFrom` to convert the
JSON formatted string to a `Map`.

```java
BatchStage<Map<String, Object>> personsAsMap = stage.map(jsonString -> JsonUtil.mapFrom(jsonString));
```

You can also use supported annotations from
[Jackson Annotations](https://github.com/FasterXML/jackson-annotations/wiki/Jackson-Annotations)
in your transforms by adding it to the classpath.

<!--DOCUSAURUS_CODE_TABS-->

<!--Gradle-->

```groovy
compile 'com.fasterxml.jackson.core:jackson-annotations:2.11.0'
```

<!--Maven-->

```xml
<dependency>
  <groupId>com.fasterxml.jackson.core</groupId>
  <artifactId>jackson-annotations</artifactId>
  <version>2.11.0</version>
</dependency>
```

<!--END_DOCUSAURUS_CODE_TABS-->

For example if your bean field names differ from the JSON
string field names you can use `JsonProperty` annotation for mapping.

```java
public class Person {

    @JsonProperty("name")
    public String _name;

    @JsonProperty("age")
    public int _age;

    public Person() {
    }
}
```

See a list of [supported annotations](https://github.com/FasterXML/jackson-jr/tree/master/jr-annotation-support#supported-annotations).
