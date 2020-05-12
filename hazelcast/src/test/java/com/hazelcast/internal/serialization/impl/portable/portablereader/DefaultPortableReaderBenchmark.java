/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.serialization.impl.portable.portablereader;

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

import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderQuickTest.NON_EMPTY_PORSCHE;
import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderQuickTest.PORSCHE;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class DefaultPortableReaderBenchmark extends HazelcastTestSupport {

    private static final int WARMUP_ITERATIONS_COUNT = 500;
    private static final int MEASUREMENT_ITERATIONS_COUNT = 2000;

    private PortableReader reader;
    private PortableReader primitiveReader;
    private InternalSerializationService ss;

    @Setup
    public void setup() throws Exception {
        ss = new DefaultSerializationServiceBuilder()
                .addPortableFactory(DefaultPortableReaderTestStructure.TestPortableFactory.ID, new DefaultPortableReaderQuickTest.TestPortableFactory())
                .build();

        Portable primitive = new DefaultPortableReaderTestStructure.PrimitivePortable();
        primitiveReader = reader(primitive);

        reader = reader(PORSCHE);
    }

    private PortableReader reader(Portable portable) throws Exception {
        ss.createPortableReader(ss.toData(NON_EMPTY_PORSCHE));
        return ss.createPortableReader(ss.toData(portable));
    }

    @Benchmark
    public Object readByte() throws Exception {
        return primitiveReader.readByte("byte_");
    }

    @Benchmark
    public Object readShort() throws Exception {
        return primitiveReader.readShort("short_");
    }

    @Benchmark
    public Object readInt() throws Exception {
        return primitiveReader.readInt("int_");
    }

    @Benchmark
    public Object readLong() throws Exception {
        return primitiveReader.readLong("long_");
    }

    @Benchmark
    public Object readFloat() throws Exception {
        return primitiveReader.readFloat("float_");
    }

    @Benchmark
    public Object readDouble() throws Exception {
        return primitiveReader.readDouble("double_");
    }

    @Benchmark
    public Object readBoolean() throws Exception {
        return primitiveReader.readBoolean("boolean_");
    }

    @Benchmark
    public Object readChar() throws Exception {
        return primitiveReader.readChar("char_");
    }

    @Benchmark
    public Object readUTF() throws Exception {
        return primitiveReader.readUTF("string_");
    }


    @Benchmark
    public Object readByteArray() throws Exception {
        return primitiveReader.readByteArray("bytes");
    }

    @Benchmark
    public Object readShortArray() throws Exception {
        return primitiveReader.readShortArray("shorts");
    }

    @Benchmark
    public Object readIntArray() throws Exception {
        return primitiveReader.readIntArray("ints");
    }

    @Benchmark
    public Object readLongArray() throws Exception {
        return primitiveReader.readLongArray("longs");
    }

    @Benchmark
    public Object readFloatArray() throws Exception {
        return primitiveReader.readFloatArray("floats");
    }

    @Benchmark
    public Object readDoubleArray() throws Exception {
        return primitiveReader.readDoubleArray("doubles");
    }

    @Benchmark
    public Object readBooleanArray() throws Exception {
        return primitiveReader.readBooleanArray("booleans");
    }

    @Benchmark
    public Object readCharArray() throws Exception {
        return primitiveReader.readCharArray("chars");
    }

    @Benchmark
    public Object readUTFArray() throws Exception {
        return primitiveReader.readUTFArray("strings");
    }

    @Benchmark
    public Object readPortable() throws Exception {
        return reader.readPortable("engine");
    }

    @Benchmark
    public Object readPortableArray() throws Exception {
        return reader.readPortableArray("wheels");
    }

    @Benchmark
    public Object readPortableInt_nested() throws Exception {
        return reader.readInt("engine.power");
    }

    @Benchmark
    public Object readPortablePortableInt_nestedTwice() throws Exception {
        return reader.readInt("engine.chip.power");
    }

    @Benchmark
    public Object readPortableFromArray() throws Exception {
        return reader.readPortable("wheels[0]");
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(DefaultPortableReaderBenchmark.class.getSimpleName())
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
