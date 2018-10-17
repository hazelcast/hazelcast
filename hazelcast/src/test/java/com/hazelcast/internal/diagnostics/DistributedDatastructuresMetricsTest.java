/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.Collections.singletonList;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DistributedDatastructuresMetricsTest extends HazelcastInstanceMetricsIntegrationTest {

    private static final int EVENT_COUNTER = 1000;

    private static final String MAP_NAME = "myMap";
    private static final String MULTI_MAP_NAME = "myMultiMap";
    private static final String EXECUTOR_NAME = "myExecutor";
    private static final String QUEUE_NAME = "myQueue";
    private static final String REPLICATED_MAP_NAME = "myReplicatedMap";
    private static final String TOPIC_NAME = "myTopic";
    private static final String RELIABLE_TOPIC_NAME = "myReliableTopic";
    private static final String PN_COUNTER_NAME = "myPnCounter";
    private static final String FLAKE_ID_GEN_NAME = "myFlakeIdGenerator";
    private static final String CACHE_NAME = "myCache";
    private static final String NEAR_CACHE_MAP_NAME = "nearCacheMap";
    private static final String INDEX_MAP_NAME = "indexMap";

    @Override
    protected Config configure() {
        Config config = new Config()
                .setProperty(Diagnostics.METRICS_LEVEL.getName(), ProbeLevel.INFO.name())
                .setProperty(Diagnostics.METRICS_DISTRIBUTED_DATASTRUCTURES.getName(), "true");
        config.addMapConfig(new MapConfig(NEAR_CACHE_MAP_NAME).setNearCacheConfig(new NearCacheConfig("nearCache")));
        config.addMapConfig(new MapConfig(INDEX_MAP_NAME).setMapIndexConfigs(
                singletonList(new MapIndexConfig("age", true))));
        config.addCacheConfig(new CacheSimpleConfig().setName(CACHE_NAME).setStatisticsEnabled(true));
        return config;
    }

    @Test
    public void testMap() {
        final IMap<Integer, Integer> map = hz.getMap(MAP_NAME);

        Random random = new Random();
        for (int i = 0; i < EVENT_COUNTER; i++) {
            int key = random.nextInt(Integer.MAX_VALUE);
            map.putAsync(key, 23);
            map.removeAsync(key);
        }

        assertEventuallyHasStats(18, "map", MAP_NAME);
    }

    @Test
    public void testMultiMap() {
        final MultiMap<Object, Object> map = hz.getMultiMap(MULTI_MAP_NAME);

        Random random = new Random();
        for (int i = 0; i < EVENT_COUNTER; i++) {
            int key = random.nextInt(Integer.MAX_VALUE);
            map.put(key, 23);
            map.remove(key);
        }

        assertEventuallyHasStats(18, "multiMap", MULTI_MAP_NAME);
    }

    @Test
    public void testExecutor() throws InterruptedException {
        final IExecutorService executor = hz.getExecutorService(EXECUTOR_NAME);

        final CountDownLatch latch = new CountDownLatch(EVENT_COUNTER);
        for (int i = 0; i < EVENT_COUNTER; i++) {
            executor.submit(new EmptyRunnable(), new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable t) {

                }
            });
        }
        latch.await();

        assertEventuallyHasStats(6, "executor", EXECUTOR_NAME);
    }

    @Test
    public void testQueue() {
        final IQueue<Object> q = hz.getQueue(QUEUE_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            q.offer(i);
        }
        q.poll();

        assertEventuallyHasStats(12, "queue", QUEUE_NAME);
    }

    @Test
    public void testReplicatedMap() {
        final ReplicatedMap<Object, Object> replicatedMap = hz.getReplicatedMap(REPLICATED_MAP_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            replicatedMap.put(i, i);
        }
        replicatedMap.remove(0);

        assertEventuallyHasStats(16, "replicatedMap", REPLICATED_MAP_NAME);
    }

    @Test
    public void testTopic() {
        final ITopic<Object> topic = hz.getTopic(TOPIC_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            topic.publish(i);
        }

        assertEventuallyHasStats(3, "topic", TOPIC_NAME);
    }

    @Test
    public void testReliableTopic() {
        final ITopic<Object> topic = hz.getReliableTopic(RELIABLE_TOPIC_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            topic.publish(i);
        }

        assertEventuallyHasStats(3, "reliableTopic", RELIABLE_TOPIC_NAME);
    }

    @Test
    public void testPnCounter() {
        final PNCounter counter = hz.getPNCounter(PN_COUNTER_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            counter.incrementAndGet();
        }

        assertEventuallyHasStats(4, "pNCounter", PN_COUNTER_NAME);
    }

    @Test
    public void testFlakeIdGenerator() {
        final FlakeIdGenerator gen = hz.getFlakeIdGenerator(FLAKE_ID_GEN_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            gen.newId();
        }

        assertEventuallyHasStats(3, "flakeIdGenerator", FLAKE_ID_GEN_NAME);
    }

    @Test
    public void testCache() {
        final ICache<Object, Object> cache = hz.getCacheManager().getCache(CACHE_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            cache.put(i, i);
        }

        assertEventuallyHasStats(12, "cache", "/hz/" + CACHE_NAME);
    }

    @Test
    public void testMapNearCache() {
        final IMap<Object, Object> map = hz.getMap(NEAR_CACHE_MAP_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            map.put(i, i);
            map.get(i);
        }

        assertEventuallyHasStats(14, "map.nearcache", NEAR_CACHE_MAP_NAME);
    }

    @Test
    public void testMapIndex() {
        final IMap<Object, Object> map = hz.getMap(INDEX_MAP_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            map.put(i, new Person(i));
            map.get(i);
        }

        assertEventuallyHasStats(12, "map.index", INDEX_MAP_NAME, "index");
    }

    private static class Person implements Serializable {
        @SuppressWarnings("unused")
        final int age;

        Person(int age) {
            this.age = age;
        }
    }

    static class EmptyRunnable implements Runnable, Serializable {

        @Override
        public void run() {
        }
    }

}
