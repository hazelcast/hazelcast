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

package com.hazelcast.internal.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.backup.TestBackupUtils.assertBackupSizeEventually;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class MapExpirationStressTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule = set(PROP_TASK_PERIOD_SECONDS, "2");

    protected final String mapName = "test";

    private static final int CLUSTER_SIZE = 5;
    private static final int KEY_RANGE = 100000;

    private HazelcastInstance[] instances = new HazelcastInstance[CLUSTER_SIZE];
    private TestHazelcastInstanceFactory factory;

    private Random random = new Random();
    private final AtomicBoolean done = new AtomicBoolean();
    private final int DURATION_SECONDS = 20;

    @Before
    public void setup() {
        Config config = getConfig();
        config.addMapConfig(getMapConfig());
        factory = createHazelcastInstanceFactory(CLUSTER_SIZE);
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }
    }

    protected MapConfig getMapConfig() {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(mapName);
        mapConfig.setMaxIdleSeconds(2);
        mapConfig.setBackupCount(CLUSTER_SIZE - 1);
        return mapConfig;
    }

    @Test
    public void test() throws InterruptedException {
        assertClusterSize(CLUSTER_SIZE, instances);
        List<Thread> list = new ArrayList<>();
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            list.add(new Thread(new TestRunner(instances[i].getMap(mapName), done)));
        }

        for (Thread thread : list) {
            thread.start();
        }

        sleepAtLeastSeconds(DURATION_SECONDS);

        done.set(true);
        for (Thread thread : list) {
            thread.join();
        }

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
        int op = random.nextInt(3);
        int key = random.nextInt(KEY_RANGE);
        int val = random.nextInt(KEY_RANGE);
        switch (op) {
            case 0:
                map.put(key, val);
                break;
            case 1:
                map.remove(key);
                break;
            case 2:
                map.get(key);
                break;
            default:
                map.get(key);
                break;
        }
    }

    class TestRunner implements Runnable {
        private IMap map;
        private AtomicBoolean done;

        TestRunner(IMap map, AtomicBoolean done) {
            this.map = map;
            this.done = done;
        }

        @Override
        public void run() {
            while (!done.get()) {
                doOp(map);
            }
        }
    }
}
