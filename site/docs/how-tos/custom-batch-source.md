---
title: Add Batching to Custom Source
description: Improve throughput by adding batching to a custom data source
---

In the [Custom Sources and
Sinks](../api/sources-sinks.md#custom-sources-and-sinks) section of our
[Sources and Sinks](../api/sources-sinks.md) programming guide we have
seen some basic examples of user-defined sources and sinks. Here we
extend this topic by showing you how to add batching

## 1. Define the Source

Here is how you create a source that reads lines of text from a file:

```java
public class Sources {

    public static BatchSource<String> buildLineSource() {
        return SourceBuilder
                .batch("line-source", x ->
                        new BufferedReader(new FileReader("lines.txt")))
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

Now it is ready to be used from the pipeline, just like a built-in
source:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.buildLineSource())
 .writeTo(Sinks.logger());
```

## 2. Add Batching

While this simple source functions correctly, it's not optimal because
it reads just one line and returns. There are some overheads to
repeatedly calling `fillBufferFn` and you can add more than one item to
the buffer:

```java
SourceBuilder
    .batch("line-source",
            x -> new BufferedReader(new FileReader("lines.txt")))
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

This code adds 128 lines at a time. In your specific case you should
use the rule of thumb to target about 1 millisecond spent in a single
invocation of `fillBufferFn`.
