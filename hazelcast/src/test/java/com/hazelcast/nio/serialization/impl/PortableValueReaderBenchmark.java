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

package com.hazelcast.nio.serialization.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.InternalValueReader;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.impl.PortableValueReaderQuickTest.TestPortableFactory;
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

import static com.hazelcast.nio.serialization.impl.PortableValueReaderQuickTest.NON_EMPTY_PORSCHE;
import static com.hazelcast.nio.serialization.impl.PortableValueReaderQuickTest.PORSCHE;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class PortableValueReaderBenchmark extends HazelcastTestSupport {

    private static final int WARMUP_ITERATIONS_COUNT = 500;
    private static final int MEASUREMENT_ITERATIONS_COUNT = 2000;

    private InternalValueReader reader;
    private InternalValueReader primitiveReader;
    private InternalSerializationService ss;

    @Setup
    public void setup() throws Exception {
        ss = new DefaultSerializationServiceBuilder()
                .addPortableFactory(TestPortableFactory.ID, new TestPortableFactory())
                .build();

        Portable primitive = new PortableValueReaderTestStructure.PrimitivePortable();
        primitiveReader = reader(primitive);
        reader = reader(PORSCHE);
    }

    private InternalValueReader reader(Portable portable) throws Exception {
        ss.createPortableValueReader(ss.toData(NON_EMPTY_PORSCHE));
        return ss.createPortableValueReader(ss.toData(portable));

    }

    public Object readByte() throws Exception {
        return primitiveReader.read("byte_");
    }

    @Benchmark
    public Object readShort() throws Exception {
        return primitiveReader.read("short_");
    }

    @Benchmark
    public Object readInt() throws Exception {
        return primitiveReader.read("int_");
    }

    @Benchmark
    public Object readLong() throws Exception {
        return primitiveReader.read("long_");
    }

    @Benchmark
    public Object readFloat() throws Exception {
        return primitiveReader.read("float_");
    }

    @Benchmark
    public Object readDouble() throws Exception {
        return primitiveReader.read("double_");
    }

    @Benchmark
    public Object readBoolean() throws Exception {
        return primitiveReader.read("boolean_");
    }

    @Benchmark
    public Object readChar() throws Exception {
        return primitiveReader.read("char_");
    }

    @Benchmark
    public Object readUTF() throws Exception {
        return primitiveReader.read("string_");
    }


    @Benchmark
    public Object readByteArray() throws Exception {
        return primitiveReader.read("bytes");
    }

    @Benchmark
    public Object readShortArray() throws Exception {
        return primitiveReader.read("shorts");
    }

    @Benchmark
    public Object readIntArray() throws Exception {
        return primitiveReader.read("ints");
    }

    @Benchmark
    public Object readLongArray() throws Exception {
        return primitiveReader.read("longs");
    }

    @Benchmark
    public Object readFloatArray() throws Exception {
        return primitiveReader.read("floats");
    }

    @Benchmark
    public Object readDoubleArray() throws Exception {
        return primitiveReader.read("doubles");
    }

    @Benchmark
    public Object readBooleanArray() throws Exception {
        return primitiveReader.read("booleans");
    }

    @Benchmark
    public Object readCharArray() throws Exception {
        return primitiveReader.read("chars");
    }

    @Benchmark
    public Object readUTFArray() throws Exception {
        return primitiveReader.read("strings");
    }

    @Benchmark
    public Object readPortable() throws Exception {
        return reader.read("engine");
    }

    @Benchmark
    public Object readPortableArray() throws Exception {
        return reader.read("wheels");
    }

    @Benchmark
    public Object readPortableInt_nested() throws Exception {
        return reader.read("engine.power");
    }

    @Benchmark
    public Object readPortablePortableInt_nestedTwice() throws Exception {
        return reader.read("engine.chip.power");
    }

    @Benchmark
    public Object readPortableFromArray() throws Exception {
        return reader.read("wheels[0]");
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(PortableValueReaderBenchmark.class.getSimpleName())
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
