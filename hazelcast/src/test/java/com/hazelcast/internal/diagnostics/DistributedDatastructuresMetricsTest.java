/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ITopic;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

import static com.hazelcast.cache.CacheUtil.getDistributedObjectName;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DistributedDatastructuresMetricsTest extends HazelcastTestSupport {

    private static final int EVENT_COUNTER = 1000;

    private static final String MAP_NAME = "myMap";
    private static final String CACHE_NAME = "myCache";
    private static final String EXECUTOR_NAME = "myExecutor";
    private static final String QUEUE_NAME = "myQueue";
    private static final String REPLICATED_MAP_NAME = "myReplicatedMap";
    private static final String TOPIC_NAME = "myTopic";
    private static final String NEAR_CACHE_MAP_NAME = "nearCacheMap";
    private HazelcastInstance hz;

    @Before
    public void setup() {
        Config config = new Config();
        config.addMapConfig(new MapConfig(NEAR_CACHE_MAP_NAME).setNearCacheConfig(new NearCacheConfig("nearCache")));
        config.getMetricsConfig()
              .setMetricsForDataStructuresEnabled(true)
              .setMinimumLevel(ProbeLevel.INFO);
        config.addCacheConfig(new CacheSimpleConfig()
                .setName(CACHE_NAME)
                .setStatisticsEnabled(true));

        hz = createHazelcastInstance(config);

        warmUpPartitions(hz);
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

        assertHasStatsEventually(MAP_NAME, "map.");
    }

    @Test
    public void testCache() {
        final ICache<Object, Object> cache = hz.getCacheManager().getCache(CACHE_NAME);

        Random random = new Random();
        for (int i = 0; i < EVENT_COUNTER; i++) {
            int key = random.nextInt(Integer.MAX_VALUE);
            cache.putAsync(key, 23);
            cache.removeAsync(key);
        }

        assertHasStatsEventually(getDistributedObjectName(CACHE_NAME), "cache.");
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

        assertHasStatsEventually(EXECUTOR_NAME, "executor.");
    }

    @Test
    public void testQueue() {
        final IQueue<Object> q = hz.getQueue(QUEUE_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            q.offer(i);
        }
        q.poll();

        assertHasStatsEventually(QUEUE_NAME, "queue.");
    }

    @Test
    public void testReplicatedMap() {
        final ReplicatedMap<Object, Object> replicatedMap = hz.getReplicatedMap(REPLICATED_MAP_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            replicatedMap.put(i, i);
        }
        replicatedMap.remove(0);

        assertHasStatsEventually(REPLICATED_MAP_NAME, "replicatedMap.");
    }

    @Test
    public void testTopic() {
        final ITopic<Object> topic = hz.getTopic(TOPIC_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            topic.publish(i);
        }

        assertHasStatsEventually(TOPIC_NAME, "topic.");
    }

    @Test
    public void testNearCache() {
        final IMap<Object, Object> map = hz.getMap(NEAR_CACHE_MAP_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            map.put(i, i);
            map.get(i);
        }

        assertHasStatsEventually(NEAR_CACHE_MAP_NAME, "map.nearcache");
    }

    private void assertHasStatsEventually(final String dsName, final String metricPrefix) {
        final MetricsRegistry registry = getNode(hz).nodeEngine.getMetricsRegistry();
        assertTrueEventually(() -> {
            final StringMetricsCollector collector = new StringMetricsCollector(dsName, metricPrefix);
            registry.collect(collector);
            assertFalse(collector.probes.isEmpty());
        });
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
