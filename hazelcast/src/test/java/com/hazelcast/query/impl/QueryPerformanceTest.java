/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.nio.serialization.impl.DefaultPortableReaderQuickTest;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.query.extractor.ValueReader;
import com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure;
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

import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.finger;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.limb;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.person;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.tattoos;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class QueryPerformanceTest extends HazelcastTestSupport {

    private static final int WARMUP_ITERATIONS_COUNT = 1000;
    private static final int MEASUREMENT_ITERATIONS_COUNT = 10000;

    private HazelcastInstanceProxy hz;
    private IMap portableMap;
    private IMap portableMapWithExtractor;
    private IMap objectMap;
    private IMap objectMapWithExtractor;

    public static class NameExtractor extends ValueExtractor<ComplexTestDataStructure.Person, Object> {
        @Override
        public void extract(ComplexTestDataStructure.Person target, Object argument, ValueCollector collector) {
            collector.addObject(target.getName());
        }
    }

    public static class LimbNameExtractor extends ValueExtractor<ComplexTestDataStructure.Person, Object> {
        @Override
        public void extract(ComplexTestDataStructure.Person target, Object argument, ValueCollector collector) {
            collector.addObject(target.getFirstLimb().getName());
        }
    }

    public static class PortableNameExtractor extends ValueExtractor<ValueReader, Object> {
        @Override
        public void extract(ValueReader target, Object argument, ValueCollector collector) {
            target.read("name", collector);
        }
    }

    public static class PortableLimbNameExtractor extends ValueExtractor<ValueReader, Object> {
        @Override
        public void extract(ValueReader target, Object argument, ValueCollector collector) {
            target.read("firstLimb.name", collector);
        }
    }

    @Setup
    public void setup() throws IOException {
        Config config = new Config();
        config.getSerializationConfig().addPortableFactory(DefaultPortableReaderQuickTest.TestPortableFactory.ID,
                new DefaultPortableReaderQuickTest.TestPortableFactory());

        //
        // object map
        //
        MapAttributeConfig nameWithExtractor = new MapAttributeConfig();
        nameWithExtractor.setName("nameWithExtractor");
        nameWithExtractor.setExtractor("com.hazelcast.query.impl.QueryPerformanceTest$NameExtractor");

        MapAttributeConfig limbNameWithExtractor = new MapAttributeConfig();
        limbNameWithExtractor.setName("limbNameWithExtractor");
        limbNameWithExtractor.setExtractor("com.hazelcast.query.impl.QueryPerformanceTest$LimbNameExtractor");

        MapConfig objectMapConfig = new MapConfig();
        objectMapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        objectMapConfig.setName("objectMap");

        MapConfig objectMapWithExtractorConfig = new MapConfig();
        objectMapWithExtractorConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        objectMapWithExtractorConfig.setName("objectMapWithExtractor");
        objectMapWithExtractorConfig.addMapAttributeConfig(nameWithExtractor);
        objectMapWithExtractorConfig.addMapAttributeConfig(limbNameWithExtractor);

        //
        // portable map
        //
        MapAttributeConfig portableNameWithExtractor = new MapAttributeConfig();
        portableNameWithExtractor.setName("nameWithExtractor");
        portableNameWithExtractor.setExtractor("com.hazelcast.query.impl.QueryPerformanceTest$PortableNameExtractor");

        MapAttributeConfig portableLimbNameWithExtractor = new MapAttributeConfig();
        portableLimbNameWithExtractor.setName("limbNameWithExtractor");
        portableLimbNameWithExtractor.setExtractor("com.hazelcast.query.impl.QueryPerformanceTest$PortableLimbNameExtractor");

        MapConfig portableMapConfig = new MapConfig();
        portableMapConfig.setName("portableMapWithExtractor");
        portableMapConfig.addMapAttributeConfig(portableNameWithExtractor);
        portableMapConfig.addMapAttributeConfig(portableLimbNameWithExtractor);


        config.addMapConfig(objectMapConfig);
        config.addMapConfig(objectMapWithExtractorConfig);
        config.addMapConfig(portableMapConfig);
        hz = (HazelcastInstanceProxy) createHazelcastInstance(config);

        portableMap = hz.getMap("portableMap");
        objectMap = hz.getMap("objectMap");
        objectMapWithExtractor = hz.getMap("objectMapWithExtractor");
        portableMapWithExtractor = hz.getMap("portableMapWithExtractor");

        ComplexTestDataStructure.Person BOND = person("Bond",
                limb("left-hand", tattoos(), finger("thumb"), finger(null)),
                limb("right-hand", tattoos("knife"), finger("middle"), finger("index"))
        );

        for (int i = 0; i <= 1000; i++) {
            portableMap.put(String.valueOf(i), BOND.getPortable());
            portableMapWithExtractor.put(String.valueOf(i), BOND.getPortable());
            objectMap.put(String.valueOf(i), BOND);
            objectMapWithExtractor.put(String.valueOf(i), BOND);
        }
    }

    @TearDown
    public void tearDown() throws IOException {
        hz.shutdown();
    }

    @Benchmark
    public Object query_portable_equalsPredicate() throws IOException {
        return portableMap.values(Predicates.equal("name", "Ferrari"));
    }

    @Benchmark
    public Object query_portable_nested_equalsPredicate() throws IOException {
        return portableMap.values(Predicates.equal("firstLimb.name", "Ferrari"));
    }

    @Benchmark
    public Object query_portable_nested_collection_equalsPredicate() throws IOException {
        return portableMap.values(Predicates.equal("limbs_portable[1].name", "Ferrari"));
    }

    @Benchmark
    public Object query_portable_nestedTwice_collection_equalsPredicate() throws IOException {
        return portableMap.values(Predicates.equal("limbs_portable[0].fingers_portable[1].name", "Ferrari"));
    }

    @Benchmark
    public Object query_portable_extractor_equalsPredicate() throws IOException {
        return portableMapWithExtractor.values(Predicates.equal("nameWithExtractor", "Ferrari"));
    }

    @Benchmark
    public Object query_portable_extractor_nested_equalsPredicate() throws IOException {
        return portableMapWithExtractor.values(Predicates.equal("limbNameWithExtractor", "Ferrari"));
    }

    @Benchmark
    public Object query_object_equalsPredicate() throws IOException {
        return objectMap.values(Predicates.equal("name", "Ferrari"));
    }

    @Benchmark
    public Object query_object_nested_equalsPredicate() throws IOException {
        return objectMap.values(Predicates.equal("firstLimb.name", "Ferrari"));
    }

    @Benchmark
    public Object query_object_nested_collection_equalsPredicate() throws IOException {
        return objectMap.values(Predicates.equal("limbs_list[1].name", "Ferrari"));
    }

    @Benchmark
    public Object query_object_nestedTwice_collection_equalsPredicate() throws IOException {
        return objectMap.values(Predicates.equal("limbs_list[0].fingers_list[1].name", "Ferrari"));
    }

    @Benchmark
    public Object query_object_extractor_equalsPredicate() throws IOException {
        return objectMapWithExtractor.values(Predicates.equal("nameWithExtractor", "Ferrari"));
    }

    @Benchmark
    public Object query_object_extractor_nested_equalsPredicate() throws IOException {
        return objectMapWithExtractor.values(Predicates.equal("limbNameWithExtractor", "Ferrari"));
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QueryPerformanceTest.class.getSimpleName())
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
