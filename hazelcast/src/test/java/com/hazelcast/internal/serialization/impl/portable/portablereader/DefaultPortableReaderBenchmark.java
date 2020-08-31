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
import com.hazelcast.internal.serialization.impl.GenericRecordQueryReader;
import com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderQuickTest.TestPortableFactory;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.test.HazelcastTestSupport;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderQuickTest.NON_EMPTY_PORSCHE;
import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderQuickTest.PORSCHE;

@BenchmarkMode(Mode.AverageTime)
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 5, time = 1)
public class DefaultPortableReaderBenchmark extends HazelcastTestSupport {

    private GenericRecordQueryReader reader;
    private GenericRecordQueryReader primitiveReader;
    private InternalSerializationService ss;

    @Setup
    public void setup() throws Exception {
        ss = new DefaultSerializationServiceBuilder()
                .addPortableFactory(TestPortableFactory.ID, new TestPortableFactory())
                .build();

        Portable primitive = new DefaultPortableReaderTestStructure.PrimitivePortable(1, DefaultPortableReaderTestStructure.PrimitivePortable.Init.FULL);
        primitiveReader = reader(primitive);
        reader = reader(PORSCHE);
    }

    private GenericRecordQueryReader reader(Portable portable) throws Exception {
        ss.readAsInternalGenericRecord(ss.toData(NON_EMPTY_PORSCHE));
        return new GenericRecordQueryReader(ss.readAsInternalGenericRecord(ss.toData(portable)));

    }

    @Benchmark
    public Object read() throws Exception {
//        return primitiveReader.read("byte_");
//        return primitiveReader.read("longs[2]");
//        return primitiveReader.read("strings[2]");
//        return reader.read("wheels[any].name");
        return reader.read("engine.chip.power");
    }

    public static void main(String[] args) throws Exception {
        DefaultPortableReaderBenchmark defaultPortableReaderBenchmark = new DefaultPortableReaderBenchmark();
        defaultPortableReaderBenchmark.setup();
        System.out.println(defaultPortableReaderBenchmark.read());

        Options opt = new OptionsBuilder()
                .include(DefaultPortableReaderBenchmark.class.getSimpleName())
                .forks(1)
                .threads(5)
                .build();

        new Runner(opt).run();
    }
}
