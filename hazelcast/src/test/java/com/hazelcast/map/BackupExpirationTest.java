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
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Map;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class BackupExpirationTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "test";
    private static final int NODE_COUNT = 3;
    private static final int REPLICA_COUNT = NODE_COUNT;

    private HazelcastInstance[] nodes;

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameters(name = "inMemoryFormat:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {BINARY}, {OBJECT},
        });
    }

    protected void configureAndStartNodes(int maxIdleSeconds, int partitionCount,
                                          int expirationTaskPeriodSeconds) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);

        Config config = getConfig();
        config.setProperty(PARTITION_COUNT.getName(), Integer.toString(partitionCount));
        config.setProperty(PROP_TASK_PERIOD_SECONDS, Integer.toString(expirationTaskPeriodSeconds));
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.setBackupCount(NODE_COUNT - 1);
        mapConfig.setMaxIdleSeconds(maxIdleSeconds);
        mapConfig.setInMemoryFormat(inMemoryFormat);

        nodes = factory.newInstances(config);
    }

    @Test
    public void all_backups_should_be_empty_eventually() throws Exception {
        configureAndStartNodes(3, 271, 5);

        IMap map = nodes[0].getMap(MAP_NAME);
        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }

        sleepSeconds(5);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance node : nodes) {
                    assertEquals(0, getTotalEntryCount(node.getMap(MAP_NAME).getLocalMapStats()));
                }
            }
        });
    }

    @Test
    public void updates_on_same_key_prevents_expiration_on_backups() throws Exception {
        configureAndStartNodes(10, 1, 1);

        IMap map = nodes[0].getMap(MAP_NAME);
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);

        sleepSeconds(11);

        map.get(1);
        map.get(2);
        map.get(3);

        map.put(1, 1);

        sleepSeconds(5);

        int total = 0;
        for (HazelcastInstance node : nodes) {
            total += getTotalEntryCount(node.getMap(MAP_NAME).getLocalMapStats());
        }

        // key 1 should still be in all replicas
        assertEquals(REPLICA_COUNT, total);
    }

    @Test
    public void dont_collect_expired_keys_if_expiration_reason_is_TTL() throws Exception {
        configureAndStartNodes(30, 1, 5);

        IMap map = nodes[0].getMap(MAP_NAME);
        Object expirationQueueLength = map.executeOnKey("1", new BackupExpirationQueueLengthFinder());

        assertEquals(0, ((Integer) expirationQueueLength).intValue());
    }

    // This EP mimics TTL expiration process by creating a record
    // which expires after 100 millis. TTL expired key should not be added to the expiration queue
    // after `recordStore.get`.
    public static final class BackupExpirationQueueLengthFinder
            extends AbstractEntryProcessor implements HazelcastInstanceAware {

        private transient HazelcastInstance node;

        @Override
        public Object process(Map.Entry entry) {
            NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(node);
            SerializationService ss = nodeEngineImpl.getSerializationService();
            MapService mapService = nodeEngineImpl.getService(MapService.SERVICE_NAME);
            MapServiceContext mapServiceContext = mapService.getMapServiceContext();
            RecordStore recordStore = mapServiceContext.getRecordStore(0, MAP_NAME);

            Data dataKey = ss.toData(1);
            recordStore.put(dataKey, "value", 100, -1);
            sleepSeconds(1);
            recordStore.get(dataKey, false, null);

            InvalidationQueue expiredKeys = recordStore.getExpiredKeys();
            return expiredKeys.size();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.node = hazelcastInstance;
        }
    }

    public static long getTotalEntryCount(LocalMapStats localMapStats) {
        long ownedEntryCount = localMapStats.getOwnedEntryCount();
        long backupEntryCount = localMapStats.getBackupEntryCount();
        return ownedEntryCount + backupEntryCount;
    }
}
