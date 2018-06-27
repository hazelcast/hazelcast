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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.backup.BackupAccessor;
import com.hazelcast.test.backup.TestBackupUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.backup.TestBackupUtils.assertBackupEntryEqualsEventually;
import static com.hazelcast.test.backup.TestBackupUtils.assertBackupEntryNullEventually;

@Category({QuickTest.class, ParallelTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class MapSetTTLBackupTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;
    private final int NINSTANCE = 3;
    private HazelcastInstance[] instances;

    @Parameterized.Parameters(name = "inMemoryFormat: {0}")
    public static Object[] memoryFormat() {
        return new Object[] {InMemoryFormat.BINARY, InMemoryFormat.OBJECT};
    }

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory();
        Config config = getConfig();
        MapConfig mapConfig = new MapConfig("default");
        mapConfig.setBackupCount(NINSTANCE - 1);
        mapConfig.setInMemoryFormat(inMemoryFormat);

        config.addMapConfig(mapConfig);

        instances = new HazelcastInstance[NINSTANCE];
        for (int i = 0; i < NINSTANCE; i++) {
            instances[i] = factory.newHazelcastInstance(config);
        }
    }

    protected Config getConfig() {
        return new Config();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testBackups() {
        final String mapName = randomMapName();
        HazelcastInstance instance = instances[0];

        putKeys(instance, mapName, null, 0, 1000);
        setTTL(instance, mapName, 0, 1000, 1, TimeUnit.SECONDS);

        sleepAtLeastMillis(1001);
        for (int i = 0; i < NINSTANCE; i++) {
            assertKeysNotPresent(instances, mapName, 0, 1000);
        }
    }

    @Test
    public void testMakesTempBackupEntriesUnlimited() {
        final String mapName = randomMapName();
        HazelcastInstance instance = instances[0];

        putKeys(instance, mapName, 10, 0, 20);
        setTTL(instance, mapName, 0, 20, 0, TimeUnit.SECONDS);
        sleepAtLeastMillis(10100);
        for (int i = 0; i < NINSTANCE; i++) {
            assertKeys(instances, mapName, 0, 20);
        }
    }

    private void putKeys(HazelcastInstance instance, String mapName, Integer withTTL, int from, int to) {
        IMap map = instance.getMap(mapName);
        for (int i = from; i < to; i++) {
            if (withTTL == null) {
                map.put(i, i);
            } else {
                map.put(i, i, withTTL, TimeUnit.SECONDS);
            }
        }
    }

    private void setTTL(HazelcastInstance instance, String mapName, int from, int to, long ttl, TimeUnit timeUnit) {
        IMap map = instance.getMap(mapName);
        for (int i = from; i < to; i++) {
            map.setTTL(i, ttl, timeUnit);
        }
    }

    private void assertKeysNotPresent(HazelcastInstance[] instances, String mapName, int from, int to) {
        for (int replicaIndex = 1; replicaIndex < NINSTANCE; replicaIndex++) {
            BackupAccessor backupAccessor = TestBackupUtils.newMapAccessor(instances, mapName, replicaIndex);
            for (int i = from; i < to; i++) {
                assertBackupEntryNullEventually(i, backupAccessor);
            }
        }
    }

    private void assertKeys(HazelcastInstance[] instances, String mapName, int from, int to) {
        for (int replicaIndex = 1; replicaIndex < NINSTANCE; replicaIndex++) {
            BackupAccessor backupAccessor = TestBackupUtils.newMapAccessor(instances, mapName, replicaIndex);
            for (int i = from; i < to; i++) {
                assertBackupEntryEqualsEventually(i, i, backupAccessor);
            }
        }
    }
}
