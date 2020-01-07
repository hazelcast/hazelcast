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

package com.hazelcast.projection;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
public class QueryBenchmark {
    IMap map;

    @Setup
    public void prepare() {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig("persons");
        config.addMapConfig(mapConfig);
        mapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "age"));
        mapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "iq"));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        map = hz.getMap("persons");
        for (int k = 0; k < 100000; k++) {
            Person person = new Person(k, 100);
            map.put(k, person);
        }
    }

    @TearDown
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Benchmark
    public void testAllIndices() {
        map.keySet(Predicates.sql("age=10 and iq=100"));
    }

    @Benchmark
    public void testSuppression() {
        map.keySet(Predicates.sql("age=10 and %iq=100"));
    }

    public static class Person implements Serializable {
        private int iq;
        private int age;

        public Person(int age, int iq) {
            this.age = age;
            this.iq = iq;
        }

        public int getIq() {
            return iq;
        }

        public int getAge() {
            return age;
        }

        @Override
        public String toString() {
            return "Person{"
                    + "age=" + age
                    + ", iq=" + iq
                    + '}';
        }
    }

}

