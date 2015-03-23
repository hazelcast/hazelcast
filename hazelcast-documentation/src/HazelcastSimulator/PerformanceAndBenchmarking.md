## Performance and Benchmarking ##

Hazelcast Simulator can record throughput and latency while running a test. It uses a concept of probes. A probe can be injected into a test and it's a responsibility of the test to notify the probe about start-end of each action.

There are two classes of probes:

- SimpleProbe - It can be used to counter number of events. It doesn't have a notion of start/end.
- IntervalProbe - This probe differentiates between start/end of an action. It can be used to measure latency.

Usage:

1. Define a probe as a test property. Hazelcast Simulator will inject appropriate probe implementation.

```
public class IntIntMapTest {

    private static final ILogger log = Logger.getLogger(IntIntMapTest.class);

    private enum Operation {
        PUT,
        GET
    }

    [...]

    // Probes will be injected by Hazelcast Simulator
    public IntervalProbe intervalProbe;
    public IntervalProbe anotherIntervalProbe;
    public SimpleProbe simpleProbe;
```

2. Use the probe in your test code

```
    getLatency.started();
    map.get(key);
    getLatency.done();


```

3. Configure the probe in your test.properties:

```
probe-intervalProbe=throughput
probe-simpleProbe=throughput
```

The configuration format is specified as `probe-<nameOfField>=<type>`
Please bare in mind this is likely to change in future versions.

There are currently following implementations of IntervalProbe:

- latency - Measures latency distribution.
- maxLatency - Records a highest latency. Unlike the previous probe it records only the single highest latency measured, not a full distribution.
- hdr - Same as latency, but it does use HdrHistogram under the hood. This will replace the latency probe in future versions of Simulator.
- disabled - Dummy probe. It doesn't record anything.

There are two implementations of SimpleProbe:

- throughput - Measure throughput
- disabled - Dummy probe. It doesn't record anything.


It's important to understand a class of a probe doesn't mandate what the probe is actually measuring. Therefore tests just know a class of probe, but they don't know if the probe generates eg. a full latency histogram or just a maximum recorded latency. This is an implementation detail from a point of view of a test.