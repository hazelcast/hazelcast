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
package com.hazelcast.internal.util.phonehome;

import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.HotRestartConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.CacheTestSupport.createServerCachingProvider;
import static com.hazelcast.test.Accessors.getNode;

@State(Scope.Benchmark)
public class PhoneHomeBenchmark extends HazelcastTestSupport {

    private PhoneHome phoneHome;
    private Node node;

    @Setup
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        node = getNode(hz);
        phoneHome = new PhoneHome(node);
        CachingProvider cachingProvider = createServerCachingProvider(hz);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        for (int i = 1; i <= 5000; i++) {
            hz.getSet("set" + i);
            hz.getQueue("queue" + i);
            hz.getMultiMap("multimap" + i);
            hz.getList("list" + i);
            hz.getRingbuffer("ringbuffer" + i);
            hz.getTopic("topic" + i);
            hz.getReplicatedMap("replicatedmap" + i);
            hz.getCardinalityEstimator("cardinalityEstimator" + i);
            hz.getPNCounter("PNcounter" + i);
            hz.getFlakeIdGenerator("flakeid" + i);
            cacheManager.createCache("cache" + i, new CacheConfig<>("cache" + i));
        }

        for (int i = 1; i <= 10000; i++) {
            IMap<Object, Object> iMap = hz.getMap("map" + i);
            MapConfig config = node.getConfig().getMapConfig("map" + i);

            if (i % 10 == 0) {
                iMap.put(i, "hazelcast");
                config.getMapStoreConfig().setClassName(DelayMapStore.class.getName()).setEnabled(true);
            }
            if (i % 101 == 0) {
                config.setReadBackupData(true);
            }
            if (i % 19 == 0) {
                config.addQueryCacheConfig(new QueryCacheConfig("queryCache" + i));
            }
            if (i % 291 == 0) {
                config.addIndexConfig(new IndexConfig().setName("indexConfig" + i));
            }
            if (i % 59 == 0) {
                config.setWanReplicationRef(new WanReplicationRef().setName("wan" + i));
            }
            if (i % 101 == 0) {
                config.setHotRestartConfig(new HotRestartConfig().setEnabled(true));
            }
            if (i % 89 == 0) {
                config.setEvictionConfig(new EvictionConfig().setEvictionPolicy(EvictionPolicy.LRU));
            }
            if (i % 91 == 0) {
                config.setInMemoryFormat(InMemoryFormat.NATIVE);
            }
            if (i % 69 == 0) {
                config.getAttributeConfigs().add(new AttributeConfig("attribute" + i, AttributeExtractor.class.getName()));
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void test() {
        phoneHome.phoneHome(true);
    }

    @TearDown
    public void tear() {
        node.shutdown(true);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(PhoneHomeBenchmark.class.getName())
                .warmupIterations(5)
                .measurementIterations(5)
                .timeUnit(TimeUnit.MILLISECONDS)
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}

