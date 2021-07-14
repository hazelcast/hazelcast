---
title: Stream Deduplication with Hazelcast Jet
author: Jaromir Hamala
authorURL: https://twitter.com/jerrinot
authorImageURL: https://3l0wd94f0qdd10om8642z9se-wpengine.netdna-ssl.com/wp-content/uploads/2019/02/jaromir-hamala-170x170.png
---

Hazelcast Jet 3.2 introduces stateful map, filter, and flatmap
operations, which are very strong primitives. In this blog, I am going
to show you how to use stateful filter for detecting and removing
duplicate elements in a stream.

## Why Deduplication?

Deduplication is often used to achieve idempotency or effectively-once
delivery semantics in messaging systems. Imagine you have a
microservices architecture where individual microservices use a message
broker to communicate with each other. Achieving exactly-once semantics
is a hard problem.

If you cannot have exactly-once then you are typically left with
at-most-once and at-least-once semantics. At-most-once means messages
can get lost. This is often unacceptable. At-least-once means messages
cannot get lost, but some can be delivered more than once. This is
oftentimes better than losing messages, yet for some use cases, it’s
still not good enough. The common solution to this problem is
effectively-once. It’s essentially at-least-once combined with duplicate
detection and removal.

## Implementation Idea

The deduplication process is usually straightforward. Producers attach a
unique ID to each message. Consumers track all processed message IDs and
discard messages with already observed IDs. This is often easy for batch
processing as each batch has a finite size. Thus, it’s often feasible to
store all IDs observed in a given batch.

However, streaming systems are different beasts. Streams are
conceptually infinite and it’s not feasible to hold all observed IDs,
let alone in memory. On the other hand, it’s often sensible to assume
duplicated messages will be close to each other. Hence we can introduce
time-to-live for each ID and remove it from memory when the time-to-live
expires.

## Implementation with Hazelcast Jet

Let’s say I am running a discussion forum and I have a microservice that
sends a new message whenever a user posts a new comment. The message
looks like this:

```java
public final class AddNewComment implements Serializable {
  private UUID uuid;
  private String comment;
  private long authorId;
}
```

The UUID field is unique for each message posted. My consumer is a
Hazelcast Jet application, and I want a processing pipeline to discard
all messages with a UUID already processed in the past. It turns out to
be really trivial:

```java
stage.groupingKey(AddNewComment::getUuid)
  .filterStateful(10_000, () -> new boolean[1],
    (s, i) -> {
        boolean res = s[0];
        s[0] = true;
        return !res;
  }
);
```

How does it work? In the first step, we group a stream of incoming
comments by UUID. In the next step, we apply a filter with an array of
Booleans used as a state object. The state object will be created for
each UUID.

When a UUID is observed for the first time, the element inside the array
is false, so the code will flip it to true and the filtering function
returns true. This means the object will not be discarded.

If at some point the stream receives another comment with the same UUID,
then the filtering function receives the state object where the Boolean
inside the array is already set to true. This means the filtering
function will return false, and the duplicated object will be discarded.

The first parameter in the `filterStateful()` method is time-to-live.
Event time is typically in milliseconds. This means each state object
will be retained for at least 10 seconds. We have to choose this
parameter to match the longest possible time window between two
duplicated elements.

## Further Improvements

Let’s encapsulate the filtering logic into a reusable unit that can be
applied to an arbitrary pipeline. We are going to use the `apply()`
method to transform a pipeline. A utility class with this method is all
that’s needed:

```java
public static <T> FunctionEx<StreamStage<T>, StreamStage<T>>
deduplicationWindow(long window, FunctionEx<T, ?> extractor) {
    return stage -> stage.groupingKey(extractor)
                         .filterStateful(window, () -> new boolean[1], (s, i) -> {
                             boolean res = s[0];
                             s[0] = true;
                             return !res;
                         });
}
```

Whenever you need to add deduplication into a pipeline, you can simply call:

```java
pipelineStage.apply(
    StreamUtils.deduplicationWindow(WINDOW_LENGTH, ID_EXTRACTOR_FUNCTION)
)
[...]
```

This makes the deduplication logic independent from your business logic
and you can reuse the same deduplication utility across all your
pipelines.

## Summary

I have demonstrated the power of stateful stream processing and the
simplicity of the Jet API. It only takes a few lines of code to
implement custom stream deduplication. Visit the Hazelcast Jet page for
more info, or stop by our Gitter chat and let us know what you think!
