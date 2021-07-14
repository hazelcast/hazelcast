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

package com.hazelcast.query.impl;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.util.Random;

import static com.hazelcast.config.BitmapIndexOptions.UniqueKeyTransformation.RAW;

@State(Scope.Benchmark)
public class IndexUpdateBenchmark {

    private static final int KEY_RANGE = 1_000;

    private final Random random = new Random(303);

    private HazelcastInstance instance;
    private IMap<Integer, Integer> noIndexes;
    private IMap<Integer, Integer> singleIndex;
    private IMap<Integer, Integer> multipleIndexes;

    @Setup
    public void setup() {
        InMemoryFormat inMemoryFormat = InMemoryFormat.BINARY;
        CacheDeserializedValues cacheDeserializedValues = CacheDeserializedValues.NEVER;

        Config config = new Config();

        MapConfig noIndexesConfig = config.getMapConfig("noIndexes");
        noIndexesConfig.setInMemoryFormat(inMemoryFormat);
        noIndexesConfig.setCacheDeserializedValues(cacheDeserializedValues);

        MapConfig singleIndexConfig = config.getMapConfig("singleIndex");
        singleIndexConfig.setInMemoryFormat(inMemoryFormat);
        singleIndexConfig.setCacheDeserializedValues(cacheDeserializedValues);
        singleIndexConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "this"));

        MapConfig multipleIndexesConfig = config.getMapConfig("multipleIndexes");
        multipleIndexesConfig.setInMemoryFormat(inMemoryFormat);
        multipleIndexesConfig.setCacheDeserializedValues(cacheDeserializedValues);
        multipleIndexesConfig.addIndexConfig(new IndexConfig(IndexType.SORTED, "this"));
        multipleIndexesConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "this"));
        IndexConfig indexConfig = new IndexConfig(IndexType.BITMAP, "this");
        indexConfig.getBitmapIndexOptions().setUniqueKeyTransformation(RAW);
        multipleIndexesConfig.addIndexConfig(indexConfig);

        instance = Hazelcast.newHazelcastInstance(config);
        noIndexes = instance.getMap("noIndexes");
        singleIndex = instance.getMap("singleIndex");
        multipleIndexes = instance.getMap("multipleIndexes");
    }

    @TearDown
    public void tearDown() {
        instance.shutdown();
    }

    @Benchmark
    public void noIndexes() {
        if (random.nextBoolean()) {
            noIndexes.put(random.nextInt(KEY_RANGE), random.nextInt());
        } else {
            noIndexes.remove(random.nextInt(KEY_RANGE));
        }
    }

    @Benchmark
    public void singleIndex() {
        if (random.nextBoolean()) {
            singleIndex.put(random.nextInt(KEY_RANGE), random.nextInt());
        } else {
            singleIndex.remove(random.nextInt(KEY_RANGE));
        }
    }

    @Benchmark
    public void multipleIndexes() {
        if (random.nextBoolean()) {
            multipleIndexes.put(random.nextInt(KEY_RANGE), random.nextInt());
        } else {
            multipleIndexes.remove(random.nextInt(KEY_RANGE));
        }
    }

    public static void main(String[] args) throws RunnerException {
        // @formatter:off
        Options opt = new OptionsBuilder()
                .include(IndexUpdateBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .warmupTime(TimeValue.seconds(1))
                .measurementIterations(10)
                .measurementTime(TimeValue.seconds(1))
                .forks(1)
                .threads(1)
                .addProfiler(GCProfiler.class)
                .jvmArgsAppend("-Xmx16g")
                .build();
        // @formatter:on

        new Runner(opt).run();
    }

}
