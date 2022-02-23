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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.LocalIndexStats;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IndexStatsMoreThreadsTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.OBJECT}, {InMemoryFormat.BINARY}});
    }

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    private static final int THREADS_NUMBER = 10;
    private static final int THREADS_ITERATION = 100;

    private String mapName;
    private HazelcastInstance instance;
    private IMap<Integer, Integer> map;

    @Before
    public void before() {
        mapName = randomMapName();

        Config config = getConfig();
        config.getMapConfig(mapName).setInMemoryFormat(inMemoryFormat);

        instance = createHazelcastInstance(config);
        map = instance.getMap(mapName);
    }

    @Test
    public void testIndexStatsWithMoreThreads() throws InterruptedException {
        int entryCount = 100;
        final int lessEqualCount = 25;
        double expectedEqual = 1.0 - 1.0 / entryCount;
        double expectedGreaterEqual = 1.0 - ((double) lessEqualCount) / entryCount;

        map.addIndex(new IndexConfig(IndexType.HASH, "this").setName("this"));
        for (int i = 0; i < entryCount; ++i) {
            map.put(i, i);
        }

        assertEquals(0, stats().getQueryCount());
        assertEquals(0, stats().getIndexedQueryCount());
        assertEquals(0, valueStats().getQueryCount());
        assertEquals(0, valueStats().getHitCount());

        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch endGate = new CountDownLatch(THREADS_NUMBER);
        for (int i = 0; i < THREADS_NUMBER; i++) {
            new Thread(() -> {
                try {
                    startGate.await();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
                try {
                    for (int j = 0; j < THREADS_ITERATION; j++) {
                        map.entrySet(Predicates.alwaysTrue());
                        map.entrySet(Predicates.equal("this", 10));
                        map.entrySet(Predicates.lessEqual("this", lessEqualCount));
                    }
                } finally {
                    endGate.countDown();
                }
            }).start();
        }

        startGate.countDown();
        endGate.await();

        assertEquals(THREADS_NUMBER * THREADS_ITERATION * 3, stats().getQueryCount());
        assertEquals(THREADS_NUMBER * THREADS_ITERATION * 2, stats().getIndexedQueryCount());
        assertEquals(THREADS_NUMBER * THREADS_ITERATION * 2, valueStats().getQueryCount());
        assertEquals(THREADS_NUMBER * THREADS_ITERATION * 2, valueStats().getHitCount());
        assertEquals((expectedEqual + expectedGreaterEqual) / 2, valueStats().getAverageHitSelectivity(), 0.015);
    }

    @Test
    public void testIndexMapStatsWithMoreThreads() throws InterruptedException {
        map.addIndex(new IndexConfig(IndexType.HASH, "this").setName("this"));

        assertEquals(0, valueStats().getInsertCount());
        assertEquals(0, valueStats().getUpdateCount());
        assertEquals(0, valueStats().getRemoveCount());

        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch endGate = new CountDownLatch(THREADS_NUMBER);
        for (int i = 0; i < THREADS_NUMBER; i++) {
            final int threadOrder = i;
            new Thread(() -> {
                try {
                    startGate.await();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
                try {
                    for (int j = 0; j < THREADS_ITERATION; j++) {
                        map.put(threadOrder * THREADS_ITERATION + j, j);
                        map.put((threadOrder + THREADS_NUMBER) * THREADS_ITERATION + j, j);
                        map.remove(threadOrder * THREADS_ITERATION + j);
                        map.put((threadOrder + THREADS_NUMBER) * THREADS_ITERATION + j, j * j);
                    }
                } finally {
                    endGate.countDown();
                }
            }).start();
        }

        startGate.countDown();
        endGate.await();

        assertEquals(THREADS_NUMBER * THREADS_ITERATION * 2, valueStats().getInsertCount());
        assertEquals(THREADS_NUMBER * THREADS_ITERATION, valueStats().getUpdateCount());
        assertEquals(THREADS_NUMBER * THREADS_ITERATION, valueStats().getRemoveCount());
    }

    private LocalMapStats stats() {
        return map.getLocalMapStats();
    }

    protected LocalIndexStats valueStats() {
        return stats().getIndexStats().get("this");
    }
}
