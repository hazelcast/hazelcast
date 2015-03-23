## Performance and Benchmarking 

Hazelcast Simulator can record throughput and latency while running a test. It uses the concept of probes. A probe can be injected into a test and it is a responsibility of the test to notify the probe about start/end of each action.

There are two classes of probes:

- `SimpleProbe`: It can be used to count the number of events. It does not have a notion of start/end.
- `IntervalProbe`: Differentiates between start/end of an action. It can be used to measure latency.

The usage of probes is explained below.

1. Define a probe as a test property. Hazelcast Simulator will inject the appropriate probe implementation.

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

2. Use the probe in your test code.

   ```
    getLatency.started();
    map.get(key);
    getLatency.done();
   ```

3. Configure the probe in your `test.properties` file.

   ```
probe-intervalProbe=throughput
probe-simpleProbe=throughput
   ```

The configuration format is specified as `probe-<nameOfField>=<type>`.
Please bare in mind this format is likely to change in the future versions.

There are currently following implementations of `IntervalProbe`:

- `latency`: Measures the latency distribution.
- `maxLatency`: Records the highest latency. Unlike the previous probe, it records only the single highest latency measured, not a full distribution.
- `hdr`: Same as latency, but it uses HdrHistogram under the hood. This will replace the latency probe in future versions of Simulator.
- `disabled`: Dummy probe. It does not record anything.

There are two implementations of SimpleProbe:

- `throughput`: Measures throughput.
- `disabled`: Dummy probe. It does not record anything.


It is important to understand that a class of a probe does not mandate what the probe is actually measuring. Therefore, the tests just know a class of probe, but they do not know if the probe generates, for example, a full latency histogram or just a maximum recorded latency. This is an implementation detail from a point of view of a test.