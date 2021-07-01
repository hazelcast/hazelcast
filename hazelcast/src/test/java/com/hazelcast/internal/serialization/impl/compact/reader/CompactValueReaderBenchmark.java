/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact.reader;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.compact.CompactRecordQueryReader;
import com.hazelcast.internal.serialization.impl.compact.CompactTestUtil;
import com.hazelcast.internal.serialization.impl.compact.SchemaService;
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

import static com.hazelcast.internal.serialization.impl.compact.reader.CompactStreamSerializerValueReaderQuickTest.PORSCHE;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class CompactValueReaderBenchmark extends HazelcastTestSupport {

    private CompactRecordQueryReader reader;
    private CompactRecordQueryReader primitiveReader;
    private InternalSerializationService ss;

    @Setup
    public void setup() throws Exception {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        ss = new DefaultSerializationServiceBuilder().setSchemaService(schemaService).build();

        CompactValueReaderTestStructure.PrimitiveObject primitive = new CompactValueReaderTestStructure.PrimitiveObject();

        primitiveReader = reader(primitive);
        reader = reader(PORSCHE);
    }

    private CompactRecordQueryReader reader(Object object) throws Exception {
        Data data = ss.toData(object);
        return new CompactRecordQueryReader(ss.readAsInternalCompactRecord(data));
    }


    @Benchmark
    public Object readInt() throws Exception {
        return primitiveReader.read("int_");
    }

    @Benchmark
    public Object readPortablePortableInt_nestedTwice() throws Exception {
        return reader.read("engine.chip.power");
    }

    @Benchmark
    public Object readPortableFromArray() throws Exception {
        return reader.read("wheels[0]");
    }

    public static void main(String[] args) throws Exception {
        CompactValueReaderBenchmark compactValueReaderBenchmark = new CompactValueReaderBenchmark();
        compactValueReaderBenchmark.setup();
        compactValueReaderBenchmark.primitiveReader.read("int_");
        benchmark();
    }

    private static void benchmark() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CompactValueReaderBenchmark.class.getSimpleName())
                .warmupIterations(10)
                .warmupTime(TimeValue.seconds(2))
                .measurementIterations(5)
                .measurementTime(TimeValue.seconds(2))
                .verbosity(VerboseMode.NORMAL)
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
