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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.renderers.ProbeRenderer;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DistributedDatastructuresMetricsTest extends HazelcastTestSupport {

    private static final int EVENT_COUNTER = 1000;

    private static final String MAP_NAME = "myMap";
    private static final String EXECUTOR_NAME = "myExecutor";
    private static final String QUEUE_NAME = "myQueue";
    private static final String REPLICATED_MAP_NAME = "myReplicatedMap";
    private static final String TOPIC_NAME = "myTopic";
    private static final String NEAR_CACHE_MAP_NAME = "nearCacheMap";
    private HazelcastInstance hz;

    @Before
    public void setup() {
        Config config = new Config()
                .setProperty(Diagnostics.METRICS_LEVEL.getName(), ProbeLevel.INFO.name())
                .setProperty(Diagnostics.METRICS_DISTRIBUTED_DATASTRUCTURES.getName(), "true");
        config.addMapConfig(new MapConfig(NEAR_CACHE_MAP_NAME).setNearCacheConfig(new NearCacheConfig("nearCache")));

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

        assertHasStatsEventually("map[" + MAP_NAME + "]");
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

        assertHasStatsEventually("executor[" + EXECUTOR_NAME + "]");
    }

    @Test
    public void testQueue() {
        final IQueue<Object> q = hz.getQueue(QUEUE_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            q.offer(i);
        }
        q.poll();

        assertHasStatsEventually("queue[" + QUEUE_NAME + "]");
    }

    @Test
    public void testReplicatedMap() {
        final ReplicatedMap<Object, Object> replicatedMap = hz.getReplicatedMap(REPLICATED_MAP_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            replicatedMap.put(i, i);
        }
        replicatedMap.remove(0);

        assertHasStatsEventually("replicatedMap[" + REPLICATED_MAP_NAME + "]");
    }

    @Test
    public void testTopic() {
        final ITopic<Object> topic = hz.getTopic(TOPIC_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            topic.publish(i);
        }

        assertHasStatsEventually("topic[" + TOPIC_NAME + "]");
    }

    @Test
    public void testNearCache() {
        final IMap<Object, Object> map = hz.getMap(NEAR_CACHE_MAP_NAME);

        for (int i = 0; i < EVENT_COUNTER; i++) {
            map.put(i, i);
            map.get(i);
        }

        assertHasStatsEventually("map[" + NEAR_CACHE_MAP_NAME + "].nearcache");
    }

    private void assertHasStatsEventually(final String prefix) {
        final MetricsRegistry registry = getNode(hz).nodeEngine.getMetricsRegistry();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final StringProbeRenderer renderer = new StringProbeRenderer(prefix);
                registry.render(renderer);
                assertTrue(!renderer.probes.isEmpty());
            }
        });
    }

    static class EmptyRunnable implements Runnable, Serializable {

        @Override
        public void run() {
        }
    }

    static class StringProbeRenderer implements ProbeRenderer {
        final HashMap<String, Object> probes = new HashMap<String, Object>();
        private final String prefix;

        StringProbeRenderer(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public void renderLong(String name, long value) {
            if (name.startsWith(prefix)) {
                probes.put(name, value);
            }
        }

        @Override
        public void renderDouble(String name, double value) {
            if (name.startsWith(prefix)) {
                probes.put(name, value);
            }
        }

        @Override
        public void renderException(String name, Exception e) {
            if (name.startsWith(prefix)) {
                probes.put(name, e);
            }
        }

        @Override
        public void renderNoValue(String name) {
            if (name.startsWith(prefix)) {
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
