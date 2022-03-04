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

package com.hazelcast.internal.eviction;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.query.QueryEngineImpl;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.backup.BackupAccessor;
import com.hazelcast.test.backup.TestBackupUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.backup.TestBackupUtils.assertBackupSizeEventually;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class MapExpirationStressTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule = set(PROP_TASK_PERIOD_SECONDS, "2");

    protected final String mapName = "test";

    private static final int CLUSTER_SIZE = 3;
    private static final int KEY_RANGE = 10_000;
    private static final int THREAD_COUNT = Math.min(Runtime.getRuntime().availableProcessors(), 5);

    private HazelcastInstance[] instances = new HazelcastInstance[CLUSTER_SIZE];
    private TestHazelcastInstanceFactory factory;

    private Random random = new Random();
    private final AtomicBoolean done = new AtomicBoolean();
    private final int DURATION_SECONDS = 20;

    @Before
    public void setup() {
        Config config = getConfig();
        config.setProperty(QueryEngineImpl.DISABLE_MIGRATION_FALLBACK.getName(), "true");
        config.addMapConfig(getMapConfig());
        factory = createHazelcastInstanceFactory(CLUSTER_SIZE);
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }
    }

    protected MapConfig getMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(mapName);
        mapConfig.setMaxIdleSeconds(3);
        mapConfig.setBackupCount(CLUSTER_SIZE - 1);
        mapConfig.addIndexConfig(new IndexConfig(IndexType.SORTED, "this"));
        mapConfig.addIndexConfig(new IndexConfig(IndexType.SORTED, "__key"));
        return mapConfig;
    }

    @Test
    public void operations_run_without_exception() throws InterruptedException {
        assertClusterSize(CLUSTER_SIZE, instances);

        AtomicInteger exceptionCount = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            IMap map = instances[random.nextInt(CLUSTER_SIZE)].getMap(mapName);
            threads.add(new Thread(new TestRunner(map, done, exceptionCount)));
        }

        for (Thread thread : threads) {
            thread.start();
        }

        sleepAtLeastSeconds(DURATION_SECONDS);

        done.set(true);
        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals("Found exceptions", 0, exceptionCount.get());
        assertRecords(instances);
    }

    protected void assertRecords(final HazelcastInstance[] instances) {
        for (int i = 1; i < instances.length; i++) {
            BackupAccessor backupAccessor = TestBackupUtils.newCacheAccessor(instances, mapName, i);
            assertBackupSizeEventually(0, backupAccessor);
        }
        for (int i = 0; i < instances.length; i++) {
            final int index = i;
            assertEqualsEventually(() -> instances[index].getMap(mapName).size(), 0);
        }
        instances[0].getMap(mapName).destroy();
    }

    protected void doOp(IMap map) {
        int op = random.nextInt(7);
        int key = random.nextInt(KEY_RANGE);
        int val = random.nextInt(KEY_RANGE);
        switch (op) {
            case 0:
                map.set(key, val, 1, TimeUnit.SECONDS);
                break;
            case 1:
                map.remove(key);
                break;
            case 2:
                map.setTtl(key, 1, TimeUnit.SECONDS);
                break;
            case 3:
                map.aggregate(Aggregators.count(), Predicates.greaterThan("this", val));
                break;
            case 4:
                map.aggregate(Aggregators.count(), Predicates.greaterThan("__key", key));
                break;
            case 5:
                map.evict(key);
                break;
            case 6:
                map.values(Predicates.alwaysTrue());
                break;
            default:
                map.get(key);
                break;
        }
    }

    class TestRunner implements Runnable {
        private IMap map;
        private AtomicBoolean done;
        private AtomicInteger exceptionCount;

        TestRunner(IMap map, AtomicBoolean done, AtomicInteger exceptionCount) {
            this.map = map;
            this.done = done;
            this.exceptionCount = exceptionCount;
        }

        @Override
        public void run() {
            try {
                while (!done.get()) {
                    doOp(map);
                }
            } catch (Throwable e) {
                exceptionCount.incrementAndGet();
                throw e;
            }
        }
    }
}
