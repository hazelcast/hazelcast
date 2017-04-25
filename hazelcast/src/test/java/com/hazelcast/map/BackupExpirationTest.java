/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.eviction.ExpirationManager.SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE;
import static com.hazelcast.map.impl.eviction.ExpirationManager.SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BackupExpirationTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "test";
    private static final int NODE_COUNT = 3;

    private HazelcastInstance[] nodes;

    @Before
    public void setup() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);

        Config config = getConfig();
        config.setProperty(SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS, "1");
        config.setProperty(SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE, "100");
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.setBackupCount(NODE_COUNT - 1);
        mapConfig.setMaxIdleSeconds(2);

        nodes = factory.newInstances(config);
    }

    @Test
    public void testAllBackupsMustBeEmptyEventually() throws Exception {
        IMap map = nodes[0].getMap(MAP_NAME);
        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }

        sleepSeconds(5);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (HazelcastInstance node : nodes) {
                    assertEquals(0, entryCountOnNode(node, MAP_NAME));
                }
            }
        });
    }

    @Test
    public void testGetMustPreserveEntries() throws Exception {
        IMap map = nodes[0].getMap(MAP_NAME);
        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }

        for (int j = 0; j < 30; j++) {
            sleepSeconds(1);
            for (int i = 0; i < 10; i++) {
                System.out.println(i + " >> " + map.get(i));
            }
        }

        for (HazelcastInstance node : nodes) {
            assertEquals(10, entryCountOnNode(node, MAP_NAME));
        }

    }

    public static int entryCountOnNode(HazelcastInstance node, String mapName) {
        int size = 0;
        NodeEngineImpl nodeEngine = getNode(node).getNodeEngine();
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            final RecordStore recordStore = mapServiceContext.getExistingRecordStore(i, mapName);
            if (recordStore == null) {
                continue;
            }
            size += recordStore.size();
        }
        return size;
    }
}