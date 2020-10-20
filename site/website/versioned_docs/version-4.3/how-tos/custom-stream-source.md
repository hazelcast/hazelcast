---
title: Create a Streaming Source
description: How to create a custom streaming source for Jet.
id: version-4.3-custom-stream-source
original_id: custom-stream-source
---

In the [Custom Sources and Sinks](../api/sources-sinks.md#custom-sources-and-sinks)
section of our [Sources and Sinks](../api/sources-sinks.md) programming
guide we have seen some basic examples of user-defined sources and
sinks. Let us now examine more examples which cover some of the
trickier aspects of writing our own sources.

Here we will focus on stream sources, ones reading unbounded input data.

Let's write a source which is capable of reading lines of text from
a file. We will start with a simple version:

## 1. Define Source

```java
class Sources {

    static StreamSource<String> buildNetworkSource() {
        return SourceBuilder
                .stream("network-source", ctx -> {
                    int port = 11000;
                    ServerSocket serverSocket = new ServerSocket(port);
                    ctx.logger().info(String.format("Waiting for connection on port %d ...", port));
                    Socket socket = serverSocket.accept();
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));
                    ctx.logger().info(String.format("Data source connected on port %d.", port));
                    return new NetworkContext(reader, serverSocket);
                })
                .<String>fillBufferFn((context, buf) -> {
                    BufferedReader reader = context.getReader();
                    for (int i = 0; i < 128; i++) {
                        if (!reader.ready()) {
                            return;
                        }
                        String line = reader.readLine();
                        if (line == null) {
                            buf.close();
                            return;
                        }
                        buf.add(line);
                    }
                })
                .destroyFn(context -> context.close())
                .build();
    }

    private static class NetworkContext {

        private final BufferedReader reader;
        private final ServerSocket serverSocket;

        NetworkContext(BufferedReader reader, ServerSocket serverSocket) {
            this.reader = reader;
            this.serverSocket = serverSocket;
        }

        BufferedReader getReader() {
            return reader;
        }

        void close() {
            try {
                reader.close();
                serverSocket.close();
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

}
```

Using it in a pipeline happens just as with built-in sources:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.buildNetworkSource())
 .withoutTimestamps()
 .peek()
 .writeTo(Sinks.noop());
```

When testing it the output should look like this (let's say that the
lines we receive over the network always start with a numeric timestamp,
let's say epoch time, followed by a comma and some additional text):

```text
... Output to ordinal 0: 1583310272413,some_more_text_1
... Output to ordinal 0: 1583310272613,some_more_text_2
... Output to ordinal 0: 1583310272813,some_more_text_3
```

## 2. Add Timestamps

You might have noticed the `withoutTimestamps()` line in the previous
pipeline definition. It is needed because for stream sources Jet
has to know what kind of event timestamps they will provide (if any). Now
we are using it without timestamps, but this unfortunately means that
we aren't allowed to use [Windowed Aggregation](../tutorials/windowing.md)
in our pipeline.

There are multiple ways to fix this (we can, for example, add timestamps
in the pipeline after the source), but the most convenient one is to
provide the timestamps right in the source.

If we know that the lines of text we receive over the network are of the
same timestamped format as we've used before, we could modify our source
like this:

```java
SourceBuilder
    .timestampedStream("network-source", ctx -> {
        int port = 11000;
        ServerSocket serverSocket = new ServerSocket(port);
        ctx.logger().info(String.format("Waiting for connection on port %d ...", port));
        Socket socket = serverSocket.accept();
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        ctx.logger().info(String.format("Data source connected on port %d.", port));
        return new NetworkContext(reader, serverSocket);
    })
    .<String>fillBufferFn((context, buf) -> {
        BufferedReader reader = context.getReader();
        for (int i = 0; i < 128; i++) {
            if (!reader.ready()) {
                return;
            }

            String line = reader.readLine();
            if (line == null) {
                buf.close();
                return;
            }

            buf.add(line, Long.parseLong(line.substring(0, line.indexOf(','))));
        }
    })
    .destroyFn(context -> context.close())
    .build();
```

Using it in a pipeline definition also changes a bit:

```java
Pipeline p = Pipeline.create();
p.readFrom(Sources.buildNetworkSource())
 .withNativeTimestamps(0)
 .peek()
 .writeTo(com.hazelcast.jet.pipeline.Sinks.noop());
```

## 3. Increase Parallelism

In the examples we showed so far the source was non-distributed: Jet
will create just a single processor in the whole cluster to serve all
the data. This is an easy and obvious way to create a source connector.

If you want to create a distributed source, the challenge is
coordinating all the parallel instances to appear as a single, unified
source.

In our somewhat contrived example we could simply make each instance
listen on its own separate port. We can achieve this by modifying the
`createFn` and making use of the unique, global processor index
available in the `Processor.Context` object we get handed there:

```java
SourceBuilder
    .stream("network-source", ctx -> {
        int port = 11000 + ctx.globalProcessorIndex();
        ServerSocket serverSocket = new ServerSocket(port);
        ctx.logger().info(String.format("Waiting for connection on port %d ...", port));
        Socket socket = serverSocket.accept();
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(socket.getInputStream()));
        ctx.logger().info(String.format("Data source connected on port %d.", port));
        return new NetworkContext(reader, serverSocket);
    })
    .<String>fillBufferFn((context, buf) -> {
        BufferedReader reader = context.getReader();
        for (int i = 0; i < 128; i++) {
            if (!reader.ready()) {
                return;
            }
            String line = reader.readLine();
            if (line == null) {
                buf.close();
                return;
            }
            buf.add(line);
        }
    })
    .destroyFn(context -> context.close())
    .distributed(2)
    .build();
```

Notice that we have added an extra call to specify the local parallelism
of the source (the `distributed()` method). This means that each Jet
cluster member will now create two such sources.

When tested following lines should show up in the log:

```text
[jet] [4.3] Waiting for connection on port 11000 ...
[jet] [4.3] Waiting for connection on port 11001 ...
```

## 4. Add Fault Tolerance

If you want your source to behave correctly within a streaming Jet job
that has a processing guarantee configured (**at-least-once** or
**exactly-once**), you must help Jet with saving the operational state
of your context object to the snapshot storage.

There are two functions you must supply:

* **`createSnapshotFn`** returns a serializable object that has all the
  data you’ll need to restore the operational state
* **`restoreSnapshotFn`** applies the previously saved snapshot to the
  current context object

While a job is running, Jet calls `createSnapshotFn` at regular
intervals to save the current state.

When Jet resumes a job, it will:

* create your context object the usual way, by calling `createFn`
* retrieve the latest snapshot object from its storage
* pass the context and snapshot objects to `restoreSnapshotFn`
* start calling `fillBufferFn`, which must start by emitting the same
  item it was about to emit when createSnapshotFn was called.

You’ll find that `restoreSnapshotFn`, somewhat unexpectedly, accepts not
one but a list of snapshot objects. If you’re building a simple,
non-distributed source, this list will have just one element. However,
the same logic must work for distributed sources as well, and a
distributed source runs on many parallel processors at the same time.
Each of them will produce its own snapshot object. After a restart the
number of parallel processors may be different than before (because you
added a Jet cluster member, for example), so there’s no one-to-one
mapping between the processors before and after the restart. This is why
Jet passes all the snapshot objects to all the processors, and your
logic must work out which part of their data to use.

Here’s a brief example with a fault-tolerant streaming source that
generates a sequence of integers:

```java
StreamSource<Integer> faultTolerantSource = SourceBuilder
    .stream("fault-tolerant-source", processorContext -> new int[1])
    .<Integer>fillBufferFn((numToEmit, buffer) ->
        buffer.add(numToEmit[0]++))
    .createSnapshotFn(numToEmit -> numToEmit[0])
    .restoreSnapshotFn(
        (numToEmit, saved) -> numToEmit[0] = saved.get(0))
    .build();
```

The snapshotting function returns the current number to emit, the
restoring function sets the number from the snapshot to the current
state. This source is non-distributed, so we can safely do
`saved.get(0)`.
