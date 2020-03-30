---
title: Create a Batch Source
description: How to create a custom batch source for Jet.
id: version-4.0-custom-batch-source
original_id: custom-batch-source
---

In the [Custom Sources and Sinks](../api/sources-sinks.md#custom-sources-and-sinks)
section of our [Sources and Sinks](../api/sources-sinks.md) programming
guide we have seen some basic examples of user-defined sources and
sinks. Let us now examine more examples which cover some of the
trickier aspects of writing our own sources.

Here we will focus on batch sources, ones reading bounded input data.

## 1. Define Source

A simple custom batch source, which is capable of reading lines of text
from a file would could, for example, be constructed like this:

```java
class Sources {

    static BatchSource<String> buildLineSource() {
        return SourceBuilder
                .batch("line-source", x -> new BufferedReader(
                                                new FileReader("lines.txt")))
                .<String>fillBufferFn((in, buf) -> {
                    String line = in.readLine();
                    if (line != null) {
                        buf.add(line);
                    } else {
                        buf.close();
                    }
                })
                .destroyFn(buf -> buf.close())
                .build();
    }

}
```

Using it in a pipeline happens just as with built-in sources:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.buildLineSource())
 .writeTo(Sinks.logger());
```

## 2. Add Batching

While this simple source functions correctly, it's not efficient,
because it always just retrieves one line at a time. Optimally the
`fillBufferFn` should fill the buffer with all the items it can acquire
without blocking.

To make it more efficient we could change our `fillBufferFn` like this:

```java
SourceBuilder
    .batch("line-source", x -> new BufferedReader(
                                            new FileReader("lines.txt")))
    .<String>fillBufferFn((in, buf) -> {
        for (int i = 0; i < 128; i++) {
            String line = in.readLine();
            if (line == null) {
                buf.close();
                return;
            }
            buf.add(line);
        }
    })
    .destroyFn(buf -> buf.close())
    .build();
```
