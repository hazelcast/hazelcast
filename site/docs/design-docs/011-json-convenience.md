---
title: 011 - JSON Convenience
description: Add tooling to work with JSON formatted data.
---

*Since*: 4.2

## Summary

JSON is very frequent data exchange format. As of now, there is no
convenience for working with JSON. We should provide a simple way to
work with JSON formatted data.

## Possible Solutions

### 1. Make existing internal JSON classes public

#### Pros

- It will work out-of-the box, users won’t need to include an external
  dependency
  
#### Cons

- It doesn’t have the object mapping feature which takes raw JSON
  strings and maps it to user POJOs. Users need to work with
  `JsonObject`s to access the fields.
- The classes are in `com.hazelcast.internal.json` package, in order to
  use them in public APIs we need to relocate them in maven shade
  plugin. Shading happens at the package phase which means in the code
  we’ll see them in internal package. Though this already can be
  considered as an issue, the real problem is with the `examples`. The
  example projects has the dependency to `hazelcast-jet`, the IDE uses
  the compiled classes for this dependency, so you need to import JSON
  classes from internal packages but when you try to build from maven
  it does not compile.
  
### 2. Pick an external JSON library

#### Pros

- It will bring the object mapping capability.

#### Cons

- Extra dependency required to use it. Most of the libraries with
  object mapping capability are fat libraries.
- We should create a separate module for it or shade them to core
  module.
  
## Design

If we choose to go with the first solution, we should consider using
them as is, not relocating them to a public package. Relocation is not
good code-wise, users will see them as internal classes in our
repository but need to remember that they are moved to public package
if they want to use them. Plus we don’t have a solution for the
`examples` issue as of now.

Another option is to move them to public package in IMDG. I think this
is very unlikely but still an option to consider.

Considering the shortcomings of using internal classes, I’ve looked at
the libraries which offers object mapping capabilities and found out
that famous Jackson has this project `jackson-jr`. It is a lightweight
and featured alternative to `jackson-databind` as they claim. It is
indeed lightweight, only 400kB (100kB `jackson-jr` and 300kB
`jackson-core` which it depends on). As a note: the future CDC module
uses jackson already.

We've chosen the second solution using the `jackson-jr` as our external
JSON library. Since it is lightweight we can shade it to core module.

### Implementation

Shading the library proved to have issues because IMDG already shades
`jackson-core` which is a dependency of `jackson-jr`. We decided to
override the shaded `jackson-core` classes coming from IMDG and
re-shade them to the same location
(`com.hazelcast.com.fasterxml.jackson.`) while packaging.

Maven shade plugin filters out classes/resources shaded for
`jackson-core` in IMDG and re-shades them along with `jackson-jr`
classes/resources to the same location:

```xml
<filters>
    <filter>
        <artifact>com.hazelcast:hazelcast</artifact>
        <excludes>
            <exclude>com/hazelcast/com/fasterxml/**</exclude>
            <exclude>META-INF/services/com.hazelcast.com.fasterxml.jackson.core.JsonFactory</exclude>
        </excludes>
    </filter>
</filters>
<relocations>
    <relocation>
        <pattern>com.fasterxml.jackson.jr.</pattern>
        <shadedPattern>com.hazelcast.com.fasterxml.jackson.jr.</shadedPattern>
    </relocation>
    <relocation>
        <pattern>com.fasterxml.jackson.core.</pattern>
        <shadedPattern>com.hazelcast.com.fasterxml.jackson.core.</shadedPattern>
    </relocation>
</relocations>
```

We've also added `jackson-jr-annotation-support` library, it enables
`jackson-jr` to use annotations feature with `jackson-annotations`
library. It is just a couple of classes. If users (or another internal
module like cdc) want to use annotations, they will need to add
`jackson-annotations` dependency. If the dependency is on the classpath
we enable annotations for `jackson-jr` otherwise we use the standard
version.

```java
JSON.Builder builder = JSON.builder();
try {
    Class.forName("com.fasterxml.jackson.annotation.JacksonAnnotation", false, JsonUtil.class.getClassLoader());
    builder.register(JacksonAnnotationExtension.std);
} catch (ClassNotFoundException ignored) {
}
JSON_JR = builder.build();
```

### JSON File Source

We've used `FileSourceBuilder` to create a JSON File Source. The source
expects the content of the files as [streaming JSON](https://en.wikipedia.org/wiki/JSON_streaming)
content, where each JSON string is separated by a new-line. The JSON
string itself can span on multiple lines. The source converts each JSON
string to an object of given type or to a `Map` if no type is
specified.

```java
public static <T> BatchSource<T> json(@Nonnull String directory, @Nonnull Class<T> type) {
    return filesBuilder(directory)
            .build(path -> JsonUtil.beanSequenceFrom(path, type));
}
```

We've added a streaming source for JSON files which again uses the
`FileSourceBuilder`. The source watches the changes on the files and
converts each line appended to the given type or to a `Map` if no type
is specified.

```java
public static <T> StreamSource<T> jsonWatcher(@Nonnull String watchedDirectory, @Nonnull Class<T> type) {
    return filesBuilder(watchedDirectory)
            .buildWatcher((fileName, line) -> JsonUtil.beanFrom(line, type));
}
```

### JSON File Sink

We've used `FileSinkBuilder` to create a JSON file sink. The sink
builder expects a `toStringFn` which converts each item to a string and
writes it to as a new line:

```java
public static <T> Sink<T> json(@Nonnull String directoryName) {
    return Sinks.<T>filesBuilder(directoryName).toStringFn(JsonUtil::toJson).build();
}
```

### IMap Sink with JSON value

Hazelcast introduced `HazelcastJsonValue` as a wrapper to JSON
formatted strings. If key/value of the entry is wrapped in
`HazelcastJsonValue`, then users can run queries on these items using
the JSON structure. Currently, if user wants to wrap items to
`HazelcastJsonValue`, a prior mapping stage is necessary.

Alternatively, user can use `Sinks.mapWithMerging` or
`Sinks.mapWithEntryProcessor` to convert the key/value to
`HazelcastJsonValue` via provided functions, but these are less
performant variants since they submit entry processors per key.

We’ve changed `WriteMapP` so that it takes `toKeyFn` and `toValueFn`:

```java
public static <T, K, V> Sink<T> map(
        @Nonnull String mapName,
        @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
        @Nonnull FunctionEx<? super T, ? extends V> toValueFn
) {
    return new SinkImpl<>("mapSink(" + mapName + ')',
            writeMapP(mapName, toKeyFn, toValueFn), false, toKeyFn);
}
```

In addition to this, we’ve added a convenience for wrapping key/value
to `HazelcastJsonValue` in `JsonUtil`:

```java
public static HazelcastJsonValue hazelcastJsonValue(@Nonnull Object object) {
    return new HazelcastJsonValue(object.toString());
}
```
