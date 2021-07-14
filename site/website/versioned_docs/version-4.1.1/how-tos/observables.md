---
title: Receive Results on the Client
description: How to monitor Jet job results on the client side.
id: version-4.1.1-observables
original_id: observables
---

Usually Jet jobs make use of distributed data Sinks to output results,
in order to not bottleneck the process for massive datasets. While this
is efficient, it's not always convenient.

For certain jobs, especially batch ones, which produce a reasonably
small amount of output, some clients would prefer to get results
directly, at the point where they have submitted the job.

In order to make this happen Jet implements reactive style
`Observables`, which can be used in the following way:

```java
JetInstance jet = Jet.bootstrappedInstance();
Observable<Long> observable = jet.newObservable();
observable.addObserver(System.out::println);

Pipeline pipeline = Pipeline.create();
pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
        .writeTo(Sinks.observable(observable));

jet.newJob(pipeline).join();

observable.destroy();
```

As we can see Observables can be obtained from any Jet member or
client. An Observable can be observed by registering `Observers` on it
, which are basically callbacks for handling **results**, **errors** and
 **completion** events. Handling errors and completion is optional. In
 order to make sure that no results are lost it is recommended that the
 Observers are registered with their Observables before Job submission.

Due to their reactive nature Observers and Observables work both in
batch and streaming jobs, but for batch jobs they have even more
convenient ways to be used.

They can for example be iterated over, without needing to register any
explicit Observers up-front. They can even be converted into a `Future`
form, with a build in transformation function for the results. For
example just counting the results and printing the count is as simple
as:

```java
JetInstance jet = Jet.bootstrappedInstance();
Observable<Long> observable = jet.getObservable("results");

Pipeline pipeline = Pipeline.create();
pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
        .writeTo(Sinks.observable("results"));

observable.toFuture(Stream::count).thenAccept(System.out::println);
jet.newJob(pipeline).join();

observable.destroy();
```

## Clean-up

Observables are backed by
[Ringbuffers](/javadoc/4.1.1/com/hazelcast/ringbuffer/Ringbuffer.html)
stored in the cluster which should be cleaned up by the client, once
they are no longer necessary. They have a `destroy()` method which does
just that. If the Observable isn’t destroyed, its memory will be leaked
in the cluster forever.

In case the client crashes, or looses track of its Observables for any
reason it’s still possible to identify all live instances, by using
`JetInstance.getObservables()` and then the ones no longer needed can be
cleaned-up via their destroy methods.
