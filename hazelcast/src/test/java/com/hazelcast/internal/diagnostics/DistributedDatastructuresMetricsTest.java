/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

import com.hazelcast.cache.ICache;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ITopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cache.CacheUtil.getDistributedObjectName;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DistributedDatastructuresMetricsTest extends HazelcastTestSupport {

    private static final int EVENT_COUNTER = 1000;

    private static final String MAP_NAME = "myMap";
    private static final String MAP_NAME_NO_STAT = "myMap-noStat";
    private static final String CACHE_NAME = "myCache";
    private static final String CACHE_NAME_NO_STAT = "myCache-noStat";
    private static final String EXECUTOR_NAME = "myExecutor";
    private static final String EXECUTOR_NAME_NO_STAT = "myExecutor-noStat";
    private static final String QUEUE_NAME = "myQueue";
    private static final String QUEUE_NAME_NO_STAT = "myQueue-noStat";
    private static final String LIST_NAME = "myList";
    private static final String LIST_NAME_NO_STAT = "myList-noStat";
    private static final String SET_NAME = "mySet";
    private static final String SET_NAME_NO_STAT = "mySet-noStat";
    private static final String REPLICATED_MAP_NAME = "myReplicatedMap";
    private static final String REPLICATED_MAP_NAME_NO_STAT = "myReplicatedMap-noStat";
    private static final String TOPIC_NAME = "myTopic";
    private static final String TOPIC_NAME_NO_STAT = "myTopic-noStat";
    private static final String NEAR_CACHE_MAP_NAME = "nearCacheMap";
    private static final String NEAR_CACHE_MAP_NAME_NO_STAT = "nearCacheMap-noStat";
    private static final String PN_COUNTER_NAME = "myPNCounter";
    private static final String PN_COUNTER_NAME_NO_STAT = "myPNCounter-noStat";
    private static final String FLAKE_ID_GENERATOR_NAME = "myFlakeIdGenerator";
    private static final String FLAKE_ID_GENERATOR_NAME_NO_STAT = "myFlakeIdGenerator-noStat";
    private HazelcastInstance hz;

    @Before
    public void setup() {
        Config config = new Config();
        config.addMapConfig(new MapConfig(NEAR_CACHE_MAP_NAME).setNearCacheConfig(new NearCacheConfig("nearCache")));
        config.addCacheConfig(new CacheSimpleConfig()
                .setName(CACHE_NAME)
                .setStatisticsEnabled(true));
        config.addCacheConfig(new CacheSimpleConfig()
            .setName(CACHE_NAME_NO_STAT)
            .setStatisticsEnabled(false));

        config.getMapConfig(MAP_NAME_NO_STAT).setStatisticsEnabled(false);
        config.getMultiMapConfig(MAP_NAME_NO_STAT).setStatisticsEnabled(false);
        config.getReplicatedMapConfig(REPLICATED_MAP_NAME_NO_STAT).setStatisticsEnabled(false);
        config.getExecutorConfig(EXECUTOR_NAME_NO_STAT).setStatisticsEnabled(false);
        config.getQueueConfig(QUEUE_NAME_NO_STAT).setStatisticsEnabled(false);
        config.getListConfig(LIST_NAME_NO_STAT).setStatisticsEnabled(false);
        config.getSetConfig(SET_NAME_NO_STAT).setStatisticsEnabled(false);
        config.getTopicConfig(TOPIC_NAME_NO_STAT).setStatisticsEnabled(false);
        config.getReliableTopicConfig(TOPIC_NAME_NO_STAT).setStatisticsEnabled(false);
        config.getPNCounterConfig(PN_COUNTER_NAME_NO_STAT).setStatisticsEnabled(false);
        config.getFlakeIdGeneratorConfig(FLAKE_ID_GENERATOR_NAME_NO_STAT).setStatisticsEnabled(false);

        hz = createHazelcastInstance(config);

        warmUpPartitions(hz);
    }

    @After
    public void tearDown() {
        // explicit cleanup is required because the MBean server is static
        // cache statistics registrations will be left over when test's
        // HazelcastInstance shuts down
        destroyAllDistributedObjects(hz);
    }

    @Test
    public void testMap() {
        final IMap<Integer, Integer> map = hz.getMap(MAP_NAME);
        final IMap<Integer, Integer> mapNoStat = hz.getMap(MAP_NAME_NO_STAT);

        Random random = new Random();
        for (int i = 0; i < EVENT_COUNTER; i++) {
            int key = random.nextInt(Integer.MAX_VALUE);
            map.putAsync(key, 23);
            map.removeAsync(key);
            mapNoStat.putAsync(key, 23);
            mapNoStat.removeAsync(key);
        }

        assertHasStatsEventually(MAP_NAME, "map.");
        assertHasNoStats(MAP_NAME_NO_STAT, "map.");
    }

    @Test
    public void testMultiMap() {
        final MultiMap<Integer, Integer> map = hz.getMultiMap(MAP_NAME);
        final MultiMap<Integer, Integer> mapNoStat = hz.getMultiMap(MAP_NAME_NO_STAT);

        Random random = new Random();
        for (int i = 0; i < EVENT_COUNTER; i++) {
            int key = random.nextInt(Integer.MAX_VALUE);
            map.put(key, 23);
            map.remove(key);
            mapNoStat.put(key, 23);
            mapNoStat.remove(key);
        }

        assertHasStatsEventually(MAP_NAME, "multiMap.");
        assertHasNoStats(MAP_NAME_NO_STAT, "multiMap.");
    }

    @Test
    public void testCache() {
        final ICache<Object, Object> cache = hz.getCacheManager().getCache(CACHE_NAME);
        final ICache<Object, Object> cacheNoStat = hz.getCacheManager().getCache(CACHE_NAME_NO_STAT);

        Random random = new Random();
        for (int i = 0; i < EVENT_COUNTER; i++) {
            int key = random.nextInt(Integer.MAX_VALUE);
            cache.putAsync(key, 23);
            cache.removeAsync(key);
            cacheNoStat.putAsync(key, 23);
            cacheNoStat.removeAsync(key);
        }

        assertHasStatsEventually(getDistributedObjectName(CACHE_NAME), "cache.");
        assertHasNoStats(getDistributedObjectName(CACHE_NAME_NO_STAT), "cache.");
    }

    @Test
    public void testExecutor() throws InterruptedException {
        final IExecutorService executor = hz.getExecutorService(EXECUTOR_NAME);
        final IExecutorService executorNoStat = hz.getExecutorService(EXECUTOR_NAME_NO_STAT);

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
        final CountDownLatch latchNoStat = new CountDownLatch(EVENT_COUNTER);
        for (int i = 0; i < EVENT_COUNTER; i++) {
            executorNoStat.submit(new EmptyRunnable(), new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                    latchNoStat.countDown();
                }

                @Override
                public void onFailure(Throwable t) {

                }
            });
        }
        latch.await();
        latchNoStat.await();

        assertHasStatsEventually(EXECUTOR_NAME, "executor.pending");
        assertHasNoStats(EXECUTOR_NAME_NO_STAT, "executor.pending");
    }

    @Test
    public void testQueue() {
        final IQueue<Object> q = hz.getQueue(QUEUE_NAME);
        final IQueue<Object> qNoStat = hz.getQueue(QUEUE_NAME_NO_STAT);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            q.offer(i);
            qNoStat.offer(i);
        }
        q.poll();
        qNoStat.poll();

        assertHasStatsEventually(QUEUE_NAME, "queue.");
        assertHasNoStats(QUEUE_NAME_NO_STAT, "queue.");
    }

    @Test
    public void testList() {
        final IList<Object> list = hz.getList(LIST_NAME);
        final IList<Object> listNoStat = hz.getList(LIST_NAME_NO_STAT);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            list.add(i);
            listNoStat.add(i);
        }

        assertHasStatsEventually(LIST_NAME, "list.");
        assertHasNoStats(LIST_NAME_NO_STAT, "list.");
    }

    @Test
    public void testSet() {
        final ISet<Object> set = hz.getSet(SET_NAME);
        final ISet<Object> setNoStat = hz.getSet(SET_NAME_NO_STAT);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            set.add(i);
            setNoStat.add(i);
        }

        assertHasStatsEventually(SET_NAME, "set.");
        assertHasNoStats(SET_NAME_NO_STAT, "set.");
    }

    @Test
    public void testReplicatedMap() {
        final ReplicatedMap<Object, Object> replicatedMap = hz.getReplicatedMap(REPLICATED_MAP_NAME);
        final ReplicatedMap<Object, Object> replicatedMapNoStat = hz.getReplicatedMap(REPLICATED_MAP_NAME_NO_STAT);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            replicatedMap.put(i, i);
            replicatedMapNoStat.put(i, i);
        }
        replicatedMap.remove(0);
        replicatedMapNoStat.remove(0);

        assertHasStatsEventually(REPLICATED_MAP_NAME, "replicatedMap.");
        assertHasNoStats(REPLICATED_MAP_NAME_NO_STAT, "replicatedMap.");
    }

    @Test
    public void testTopic() {
        final ITopic<Object> topic = hz.getTopic(TOPIC_NAME);
        final ITopic<Object> topicNoStats = hz.getTopic(TOPIC_NAME_NO_STAT);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            topic.publish(i);
            topicNoStats.publish(i);
        }

        assertHasStatsEventually(TOPIC_NAME, "topic.");
        assertHasNoStats(TOPIC_NAME_NO_STAT, "topic.");
    }

    @Test
    public void testReliableTopic() {
        final ITopic<Object> topic = hz.getReliableTopic(TOPIC_NAME);
        final ITopic<Object> topicNoStat = hz.getReliableTopic(TOPIC_NAME_NO_STAT);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            topic.publish(i);
            topicNoStat.publish(i);
        }

        assertHasStatsEventually(TOPIC_NAME, "reliableTopic.");
        assertHasNoStats(TOPIC_NAME_NO_STAT, "reliableTopic.");
    }

    @Test
    public void testNearCache() {
        final IMap<Object, Object> map = hz.getMap(NEAR_CACHE_MAP_NAME);
        final IMap<Object, Object> mapNoStat = hz.getMap(NEAR_CACHE_MAP_NAME_NO_STAT);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            map.put(i, i);
            map.get(i);
            mapNoStat.put(i, i);
            mapNoStat.get(i);
        }

        assertHasStatsEventually(NEAR_CACHE_MAP_NAME, "map.nearcache");
        assertHasNoStats(NEAR_CACHE_MAP_NAME_NO_STAT, "map.nearcache");
    }

    @Test
    public void testPnCounter() {
        final PNCounter counter = hz.getPNCounter(PN_COUNTER_NAME);
        final PNCounter counterNoStat = hz.getPNCounter(PN_COUNTER_NAME_NO_STAT);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            counter.addAndGet(i);
            counterNoStat.addAndGet(i);
        }

        assertHasStatsEventually(PN_COUNTER_NAME, "pnCounter.");
        assertHasNoStats(PN_COUNTER_NAME_NO_STAT, "pnCounter.");
    }

    @Test
    public void testFlakeIdGenerator() {
        final FlakeIdGenerator idGenerator = hz.getFlakeIdGenerator(FLAKE_ID_GENERATOR_NAME);
        final FlakeIdGenerator idGeneratorNoStat = hz.getFlakeIdGenerator(FLAKE_ID_GENERATOR_NAME_NO_STAT);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            idGenerator.newId();
            idGeneratorNoStat.newId();
        }

        assertHasStatsEventually(FLAKE_ID_GENERATOR_NAME, "flakeIdGenerator.");
        assertHasNoStats(FLAKE_ID_GENERATOR_NAME_NO_STAT, "flakeIdGenerator.");
    }

    private void assertHasStatsEventually(final String dsName, final String metricPrefix) {
        final MetricsRegistry registry = getNode(hz).nodeEngine.getMetricsRegistry();
        assertTrueEventually(() -> {
            final StringMetricsCollector collector = new StringMetricsCollector(dsName, metricPrefix);
            registry.collect(collector);
            assertFalse(collector.probes.isEmpty());
        });
    }

    private void assertHasNoStats(final String dsName, final String metricPrefix) {
        final MetricsRegistry registry = getNode(hz).nodeEngine.getMetricsRegistry();
        final StringMetricsCollector collector = new StringMetricsCollector(dsName, metricPrefix);
        registry.collect(collector);
        assertTrue(collector.probes.isEmpty());
    }

    static class EmptyRunnable implements Runnable, Serializable {

        @Override
        public void run() {
        }
    }

    static class StringMetricsCollector implements MetricsCollector {
        final HashMap<String, Object> probes = new HashMap<>();
        private final Pattern pattern;

        StringMetricsCollector(String dsName, String metricPrefix) {
            this.pattern = Pattern.compile(
                    String.format(
                            "^\\[name=%s,(.+,)?metric=%s.+\\]$",
                            dsName.replace(".", "\\."),
                            metricPrefix.replace(".", "\\.")
                    )
            );
        }

        @Override
        public void collectLong(MetricDescriptor descriptor, long value) {
            String name = descriptor.toString();
            if (pattern.matcher(name).matches()) {
                probes.put(name, value);
            }
        }

        @Override
        public void collectDouble(MetricDescriptor descriptor, double value) {
            String name = descriptor.toString();
            if (pattern.matcher(name).matches()) {
                probes.put(name, value);
            }
        }

        @Override
        public void collectException(MetricDescriptor descriptor, Exception e) {
            String name = descriptor.toString();
            if (pattern.matcher(name).matches()) {
                probes.put(name, e);
            }
        }

        @Override
        public void collectNoValue(MetricDescriptor descriptor) {
            String name = descriptor.toString();
            if (pattern.matcher(name).matches()) {
                probes.put(name, null);
            }
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            for (Entry<String, Object> probe : probes.entrySet()) {
                sb.append(probe.getKey()).append(" - ").append(probe.getValue()).append("\n");
            }
            return sb.toString();
        }
    }
}
