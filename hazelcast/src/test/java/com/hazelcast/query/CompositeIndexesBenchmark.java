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

package com.hazelcast.query;

import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, warmups = 0)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class CompositeIndexesBenchmark {

    IMap<Integer, Pojo> map;

    @Setup
    public void setup() {
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig("map");

        mapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "f1"));
        mapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "f2"));
        mapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "f3", "f4"));

        mapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "f5"));
        mapConfig.addIndexConfig(new IndexConfig(IndexType.SORTED, "f6"));
        mapConfig.addIndexConfig(new IndexConfig(IndexType.SORTED, "f7", "f8"));

        this.map = Hazelcast.newHazelcastInstance(config).getMap("map");
        for (int i = 0; i < 100000; ++i) {
            this.map.put(i, new Pojo(0, i, 0, i, 0, i % 100, 0, i % 100));
        }
    }

    @TearDown
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Benchmark
    public void benchmarkRegularPointQuery() {
        map.values(Predicates.sql("f1 = 0 and f2 = 1"));
    }

    @Benchmark
    public void benchmarkCompositePointQuery() {
        map.values(Predicates.sql("f3 = 0 and f4 = 1"));
    }

    @Benchmark
    public void benchmarkRegularRangeQuery() {
        map.values(Predicates.sql("f5 = 0 and f6 < 1"));
    }

    @Benchmark
    public void benchmarkCompositeRangeQuery() {
        map.values(Predicates.sql("f7 = 0 and f8 < 1"));
    }

    public static class Pojo implements DataSerializable {

        private int f1;
        private int f3;
        private int f2;
        private int f4;

        private int f5;
        private int f6;
        private int f7;
        private int f8;

        public Pojo() {
        }

        public Pojo(int f1, int f2, int f3, int f4, int f5, int f6, int f7, int f8) {
            this.f1 = f1;
            this.f2 = f2;
            this.f3 = f3;
            this.f4 = f4;
            this.f5 = f5;
            this.f6 = f6;
            this.f7 = f7;
            this.f8 = f8;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(f1);
            out.writeInt(f2);
            out.writeInt(f3);
            out.writeInt(f4);
            out.writeInt(f5);
            out.writeInt(f6);
            out.writeInt(f7);
            out.writeInt(f8);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            f1 = in.readInt();
            f2 = in.readInt();
            f3 = in.readInt();
            f4 = in.readInt();
            f5 = in.readInt();
            f6 = in.readInt();
            f7 = in.readInt();
            f8 = in.readInt();
        }

    }

}
