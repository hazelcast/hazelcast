package com.hazelcast.nio.serialization.impl;


import com.hazelcast.config.Config;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.test.HazelcastTestSupport;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderQuickTest.NON_EMPTY_PORSCHE;
import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderQuickTest.PORSCHE;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class DefaultPortableReaderPerformanceTest extends HazelcastTestSupport {


    private static final int WARMUP_ITERATIONS_COUNT = 1000;
    private static final int MEASUREMENT_ITERATIONS_COUNT = 5000;

    private HazelcastInstanceProxy hz;
    private PortableReader reader;

    @Setup
    public void setup() throws IOException {
        Config config = new Config();
        config.getSerializationConfig().addPortableFactory(DefaultPortableReaderQuickTest.TestPortableFactory.ID,
                new DefaultPortableReaderQuickTest.TestPortableFactory());

        hz = (HazelcastInstanceProxy) createHazelcastInstance(config);
        reader = reader(PORSCHE);
    }

    @TearDown
    public void tearDown() throws IOException {
        hz.shutdown();
    }

    public PortableReader reader(Portable portable) throws IOException {
        IMap map = hz.getMap("stealingMap");

        map.put(NON_EMPTY_PORSCHE.toString(), NON_EMPTY_PORSCHE);
        map.put(portable.toString(), portable);

        DefaultPortableReaderSpecTest.EntryStealingProcessor processor = new DefaultPortableReaderSpecTest.EntryStealingProcessor(portable.toString());
        map.executeOnEntries(processor);

        SerializationServiceV1 ss = (SerializationServiceV1) hz.getSerializationService();
        return ss.createPortableReader(processor.stolenEntryData);
    }

    @Benchmark
    public Object readUTF() throws IOException {
        return reader.readUTF("name");
    }

    @Benchmark
    public Object readUTFArray() throws IOException {
        return reader.readUTFArray("model");
    }

    @Benchmark
    public Object readPortable() throws IOException {
        return reader.readPortable("engine");
    }

    @Benchmark
    public Object readPortableArray() throws IOException {
        return reader.readPortableArray("wheels");
    }

    @Benchmark
    public Object readInt_nested() throws IOException {
        return reader.readInt("engine.power");
    }

    @Benchmark
    public Object readInt_nestedTwice() throws IOException {
        return reader.readInt("engine.chip.power");
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(DefaultPortableReaderPerformanceTest.class.getSimpleName())
                .warmupIterations(WARMUP_ITERATIONS_COUNT)
                .warmupTime(TimeValue.milliseconds(2))
                .measurementIterations(MEASUREMENT_ITERATIONS_COUNT)
                .measurementTime(TimeValue.milliseconds(2))
                .verbosity(VerboseMode.NORMAL)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

}
