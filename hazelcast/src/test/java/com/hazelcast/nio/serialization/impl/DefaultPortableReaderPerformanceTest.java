package com.hazelcast.nio.serialization.impl;


import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
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

    private static final int WARMUP_ITERATIONS_COUNT = 500;
    private static final int MEASUREMENT_ITERATIONS_COUNT = 2000;

    private PortableReader reader;
    private PortableReader primitiveReader;
    private InternalSerializationService ss;


    @Setup
    public void setup() throws IOException {
        ss = new DefaultSerializationServiceBuilder()
                .addPortableFactory(DefaultPortableReaderQuickTest.TestPortableFactory.ID,
                        new DefaultPortableReaderQuickTest.TestPortableFactory()).build();

        Portable primitive = new DefaultPortableReaderTestStructure.PrimitivePortable();
        primitiveReader = reader(primitive);

        reader = reader(PORSCHE);
    }

    public PortableReader reader(Portable portable) throws IOException {
        ss.createPortableReader(ss.toData(NON_EMPTY_PORSCHE));
        return ss.createPortableReader(ss.toData(portable));
    }


    @Benchmark
    public Object readByte() throws IOException {
        return primitiveReader.readByte("byte_");
    }

    @Benchmark
    public Object readShort() throws IOException {
        return primitiveReader.readShort("short_");
    }

    @Benchmark
    public Object readInt() throws IOException {
        return primitiveReader.readInt("int_");
    }

    @Benchmark
    public Object readLong() throws IOException {
        return primitiveReader.readLong("long_");
    }

    @Benchmark
    public Object readFloat() throws IOException {
        return primitiveReader.readFloat("float_");
    }

    @Benchmark
    public Object readDouble() throws IOException {
        return primitiveReader.readDouble("double_");
    }

    @Benchmark
    public Object readBoolean() throws IOException {
        return primitiveReader.readBoolean("boolean_");
    }

    @Benchmark
    public Object readChar() throws IOException {
        return primitiveReader.readChar("char_");
    }

    @Benchmark
    public Object readUTF() throws IOException {
        return primitiveReader.readUTF("string_");
    }


    @Benchmark
    public Object readByteArray() throws IOException {
        return primitiveReader.readByteArray("bytes");
    }

    @Benchmark
    public Object readShortArray() throws IOException {
        return primitiveReader.readShortArray("shorts");
    }

    @Benchmark
    public Object readIntArray() throws IOException {
        return primitiveReader.readIntArray("ints");
    }

    @Benchmark
    public Object readLongArray() throws IOException {
        return primitiveReader.readLongArray("longs");
    }

    @Benchmark
    public Object readFloatArray() throws IOException {
        return primitiveReader.readFloatArray("floats");
    }

    @Benchmark
    public Object readDoubleArray() throws IOException {
        return primitiveReader.readDoubleArray("doubles");
    }

    @Benchmark
    public Object readBooleanArray() throws IOException {
        return primitiveReader.readBooleanArray("booleans");
    }

    @Benchmark
    public Object readCharArray() throws IOException {
        return primitiveReader.readCharArray("chars");
    }

    @Benchmark
    public Object readUTFArray() throws IOException {
        return primitiveReader.readUTFArray("strings");
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
    public Object readPortableInt_nested() throws IOException {
        return reader.readInt("engine.power");
    }

    @Benchmark
    public Object readPortablePortableInt_nestedTwice() throws IOException {
        return reader.readInt("engine.chip.power");
    }

    @Benchmark
    public Object readPortableFromArray() throws IOException {
        return reader.readPortable("wheels[0]");
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
