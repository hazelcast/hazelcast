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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.ClusterProperty;
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
@State(Scope.Thread)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
public class Query2Benchmark {
    IMap map;

    @Setup
    public void prepare() {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig("map");
        config.setProperty(ClusterProperty.QUERY_PREDICATE_PARALLEL_EVALUATION.getName(), "true");
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        mapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "f1"));
        mapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "f2"));
        mapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "f3"));
        mapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "f4"));
        config.addMapConfig(mapConfig);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        map = hz.getMap(mapConfig.getName());
        for (int k = 0; k < 100000; k++) {
            Pojo pojo = new Pojo(100, 100, 100, 100);
            map.put(k, pojo);
        }
    }

    @TearDown
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Benchmark
    public void testAllIndices() {
        map.keySet(Predicates.sql("f1=100 and f2=100 and f3=100 and f4=100 and f5=0"));
    }

    @Benchmark
    public void testSuppression() {
        map.keySet(Predicates.sql("%f1=100 and %f2=100 and %f3=100 and %f4=100 and f5=0"));
    }

    public static class Pojo implements DataSerializable {
        private int f1;
        private int f3;
        private int f2;
        private int f4;
        private int f5;

        public Pojo() {
        }

        public Pojo(int f1, int f2, int f3, int f4) {
            this.f1 = f1;
            this.f2 = f2;
            this.f3 = f3;
            this.f4 = f4;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(f1);
            out.writeInt(f2);
            out.writeInt(f3);
            out.writeInt(f4);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            f1 = in.readInt();
            f2 = in.readInt();
            f3 = in.readInt();
            f4 = in.readInt();
        }
    }

}

