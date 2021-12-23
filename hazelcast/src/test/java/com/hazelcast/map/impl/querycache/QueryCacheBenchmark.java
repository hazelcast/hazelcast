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

package com.hazelcast.map.impl.querycache;

import com.hazelcast.aggregation.Person;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.predicates.SqlPredicate;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 10, timeUnit = SECONDS)
@Measurement(iterations = 2, time = 10, timeUnit = SECONDS)
@Fork(value = 2, jvmArgsAppend = {"-Xms8G", "-Xmx8G"})
@State(Scope.Benchmark)
public class QueryCacheBenchmark {

    @Benchmark
    public Object testValues(BenchmarkContext context, Blackhole blackhole) {
        return context.getQueryCache().values(Predicates.alwaysTrue());
    }

    @Benchmark
    public Object testValuesWithPredicate(BenchmarkContext context) {
        return context.getQueryCache().values(context.getSql());
    }

    @Benchmark
    public Object testKeys(BenchmarkContext context) {
        return context.getQueryCache().keySet(Predicates.alwaysTrue());
    }

    @Benchmark
    public Object testKeysWithPredicate(BenchmarkContext context) {
        return context.getQueryCache().keySet(context.getSql());
    }

    @Benchmark
    public Object testEntries(BenchmarkContext context) {
        return context.getQueryCache().entrySet(Predicates.alwaysTrue());
    }

    @Benchmark
    public Object testEntriesWithPredicate(BenchmarkContext context) {
        return context.getQueryCache().entrySet(context.getSql());
    }

    @Benchmark
    public Object testGet(BenchmarkContext context) {
        return context.getQueryCache().get(context.getRandomKey());
    }

    @State(Scope.Benchmark)
    public static class BenchmarkContext {

        private static final String MAP_NAME = "test";

        @Param("100000")
        protected int entryCount;

        @Param({"1024"})
        protected int keySize;

        private HazelcastInstance instance;
        private Random random;
        private QueryCache queryCache;
        private SqlPredicate sql = new SqlPredicate("age > 100 AND age < 3000");
        Object[] keys;

        @Setup(Level.Trial)
        public void setUp() {
            Config config = new Config();
            MapConfig mapConfig = new MapConfig(MAP_NAME);
            QueryCacheConfig queryCacheConfig = new QueryCacheConfig(MAP_NAME);
            queryCacheConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
            queryCacheConfig.setSerializeKeys(false);
            mapConfig.addQueryCacheConfig(queryCacheConfig);
            config.addMapConfig(mapConfig);

            this.instance = Hazelcast.newHazelcastInstance(config);
            this.random = new Random();
            IMap<Object, Person> map = instance.getMap(MAP_NAME);

            byte[] bytes = new byte[4 * 1024];
            random.nextBytes(bytes);

            Object[] keyList = new Object[entryCount];

            for (int i = 0; i < entryCount; i++) {
                Object key = generateKey();
                map.set(key, new Person((double) i, bytes));
                keyList[i] = key;
            }

            keys = keyList;
            queryCache = map.getQueryCache(MAP_NAME, Predicates.alwaysTrue(), true);
        }

        private Object generateKey() {
            byte[] keyBytes = new byte[keySize];
            random.nextBytes(keyBytes);
            return keyBytes;
        }

        public QueryCache getQueryCache() {
            return queryCache;
        }

        public Object getRandomKey() {
            return keys[RandomPicker.getInt(0, entryCount)];
        }

        public Predicate getSql() {
            return sql;
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            instance.shutdown();
        }
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QueryCacheBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.JSON)
                //                .addProfiler(GCProfiler.class)
                //                .addProfiler(LinuxPerfProfiler.class)
                //                .addProfiler(HotspotMemoryProfiler.class)
                //                .shouldDoGC(true)
                //                .verbosity(VerboseMode.SILENT)
                .build();

        new Runner(opt).run();
    }

}
