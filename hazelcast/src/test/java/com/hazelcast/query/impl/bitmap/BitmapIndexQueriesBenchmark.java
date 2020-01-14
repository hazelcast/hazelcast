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

package com.hazelcast.query.impl.bitmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.Serializable;
import java.util.Random;

import static com.hazelcast.config.BitmapIndexOptions.UniqueKeyTransformation.RAW;
import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.not;
import static com.hazelcast.query.Predicates.or;

@State(Scope.Benchmark)
public class BitmapIndexQueriesBenchmark {

    private static final int SIZE = 1000;
    private static final int HABITS = 5000;
    private static final int DOMAIN = 25000;

    private final Random random = new Random(303);
    private HazelcastInstance instance;
    private IMap<Integer, Person> personsBitmap;
    private IMap<Integer, Person> personsHash;

    @Setup
    public void setup() {
        Config config = new Config();

        MapConfig personsBitmapConfig = config.getMapConfig("personsBitmap");
        personsBitmapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        IndexConfig indexConfig = new IndexConfig(IndexType.BITMAP, "habits[any]");
        indexConfig.getBitmapIndexOptions().setUniqueKeyTransformation(RAW);
        personsBitmapConfig.addIndexConfig(indexConfig);

        MapConfig personsHashConfig = config.getMapConfig("personsHash");
        personsHashConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        personsHashConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "habits[any]"));

        instance = Hazelcast.newHazelcastInstance(config);

        personsBitmap = instance.getMap("personsBitmap");
        personsHash = instance.getMap("personsHash");

        for (int i = 0; i < SIZE; ++i) {
            int[] habits = new int[HABITS];
            for (int j = 0; j < HABITS; ++j) {
                habits[j] = random.nextInt(DOMAIN);
            }
            Person person = new Person(habits);
            personsBitmap.put(i, person);
            personsHash.put(i, person);
        }
    }

    @TearDown
    public void tearDown() {
        instance.shutdown();
    }

    @Benchmark
    public void bitmapQueriesEqual() {
        personsBitmap.entrySet(equal("habits[any]", random.nextInt(DOMAIN)));
    }

    @Benchmark
    public void hashQueriesEqual() {
        personsHash.entrySet(Predicates.equal("habits[any]", random.nextInt(DOMAIN)));
    }

    @Benchmark
    public void bitmapQueriesAnd() {
        personsBitmap.entrySet(and(equal("habits[any]", random.nextInt(DOMAIN)), equal("habits[any]", random.nextInt(DOMAIN))));
    }

    @Benchmark
    public void hashQueriesAnd() {
        personsHash.entrySet(and(equal("habits[any]", random.nextInt(DOMAIN)), equal("habits[any]", random.nextInt(DOMAIN))));
    }

    @Benchmark
    public void bitmapQueriesOr() {
        personsBitmap.entrySet(or(equal("habits[any]", random.nextInt(DOMAIN)), equal("habits[any]", random.nextInt(DOMAIN))));
    }

    @Benchmark
    public void hashQueriesOr() {
        personsHash.entrySet(or(equal("habits[any]", random.nextInt(DOMAIN)), equal("habits[any]", random.nextInt(DOMAIN))));
    }

    @Benchmark
    public void bitmapQueriesNot() {
        personsBitmap.entrySet(not(equal("habits[any]", random.nextInt(DOMAIN))));
    }

    @Benchmark
    public void hashQueriesNot() {
        personsHash.entrySet(not(equal("habits[any]", random.nextInt(DOMAIN))));
    }

    public static void main(String[] args) throws RunnerException {
        // @formatter:off
        Options opt = new OptionsBuilder()
                .include(BitmapIndexQueriesBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .warmupTime(TimeValue.seconds(1))
                .measurementIterations(10)
                .measurementTime(TimeValue.seconds(1))
                .forks(1)
                .threads(1)
                .jvmArgsAppend("-Xmx16g")
                .build();
        // @formatter:on

        new Runner(opt).run();
    }

    public static class Person implements Serializable {

        private final int[] habits;

        public Person(int[] habits) {
            this.habits = habits;
        }

        @SuppressWarnings("unused")
        public int[] getHabits() {
            return habits;
        }

    }

}
