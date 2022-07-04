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
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.backup.BackupAccessor;
import com.hazelcast.test.environment.RuntimeAvailableProcessorsRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.backup.TestBackupUtils.assertBackupEntryEqualsEventually;
import static com.hazelcast.test.backup.TestBackupUtils.assertBackupEntryNullEventually;
import static com.hazelcast.test.backup.TestBackupUtils.newMapAccessor;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapSetTtlBackupTest extends HazelcastTestSupport {

    private static final int CLUSTER_SIZE = 3;

    @Parameters(name = "inMemoryFormat:{0}")
    public static Object[] memoryFormat() {
        return new Object[]{
                InMemoryFormat.BINARY,
                InMemoryFormat.OBJECT,
        };
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Rule
    public RuntimeAvailableProcessorsRule runtimeAvailableProcessorsRule = new RuntimeAvailableProcessorsRule(4);

    protected HazelcastInstance[] instances;
    private TestHazelcastInstanceFactory factory;

    @Before
    public void setup() {
        MapConfig mapConfig = new MapConfig("default")
                .setBackupCount(CLUSTER_SIZE - 1)
                .setInMemoryFormat(inMemoryFormat);

        Config config = getConfig()
                .addMapConfig(mapConfig);

        factory = createHazelcastInstanceFactory(CLUSTER_SIZE);
        instances = factory.newInstances(config);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testBackups() {
        String mapName = randomMapName();
        HazelcastInstance instance = instances[0];

        putKeys(instance, mapName, 0, 1000, 0, null);
        setTtl(instance, mapName, 0, 1000, 1, TimeUnit.SECONDS);

        sleepAtLeastMillis(1001);
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            assertKeysNotPresent(instances, mapName, 0, 1000);
        }
    }

    @Test
    public void testMakesTempBackupEntriesUnlimited() {
        String mapName = randomMapName();
        HazelcastInstance instance = instances[0];

        putKeys(instance, mapName, 0, 1000, 15, TimeUnit.SECONDS);
        setTtl(instance, mapName, 0, 1000, 0, TimeUnit.SECONDS);
        sleepAtLeastSeconds(16);
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            assertKeys(instances, mapName, 0, 1);
        }
    }

    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @SuppressWarnings("SameParameterValue")
    private static void putKeys(HazelcastInstance instance, String mapName,
                                int from, int to, int ttl, TimeUnit timeUnit) {
        IMap<Integer, Integer> map = instance.getMap(mapName);
        for (int i = from; i < to; i++) {
            if (timeUnit == null) {
                map.put(i, i);
            } else {
                map.put(i, i, ttl, timeUnit);
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static void setTtl(HazelcastInstance instance, String mapName, int from, int to, long ttl, TimeUnit timeUnit) {
        IMap<Integer, Integer> map = instance.getMap(mapName);
        for (int i = from; i < to; i++) {
            map.setTtl(i, ttl, timeUnit);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static void assertKeysNotPresent(HazelcastInstance[] instances, String mapName, int from, int to) {
        for (int replicaIndex = 1; replicaIndex < CLUSTER_SIZE; replicaIndex++) {
            BackupAccessor backupAccessor = newMapAccessor(instances, mapName, replicaIndex);
            for (int i = from; i < to; i++) {
                assertBackupEntryNullEventually(i, backupAccessor);
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static void assertKeys(HazelcastInstance[] instances, String mapName, int from, int to) {
        for (int replicaIndex = 1; replicaIndex < CLUSTER_SIZE; replicaIndex++) {
            BackupAccessor backupAccessor = newMapAccessor(instances, mapName, replicaIndex);
            for (int i = from; i < to; i++) {
                assertBackupEntryEqualsEventually(i, i, backupAccessor);
            }
        }
    }
}
