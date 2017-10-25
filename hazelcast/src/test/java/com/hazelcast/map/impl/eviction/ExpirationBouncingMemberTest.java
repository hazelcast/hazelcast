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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.eviction.ExpirationManager.SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT;
import static com.hazelcast.map.impl.eviction.ExpirationManager.SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE;
import static com.hazelcast.map.impl.eviction.ExpirationManager.SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS;
import static com.hazelcast.test.bounce.BounceTestConfiguration.DriverType.MEMBER;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ExpirationBouncingMemberTest extends HazelcastTestSupport {

    private static final int MEMBER_COUNT = 3;
    private static final int DRIVER_COUNT = 3;
    private static final int TEST_DURATION_SECONDS = 40;

    private static final int MAP_BACKUP_COUNT = 3;
    private static final String MAP_NAME = "test";

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig())
            .clusterSize(MEMBER_COUNT)
            .driverCount(DRIVER_COUNT)
            .driverType(MEMBER)
            .build();

    @Before
    public void setup() {
        HazelcastInstance steadyMember = bounceMemberRule.getSteadyMember();
        IMap<Integer, Integer> map = steadyMember.getMap(MAP_NAME);
        for (int i = 0; i < 10000; i++) {
            map.put(i, i, 1, SECONDS);
        }
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setProperty(SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS, "1");
        config.setProperty(SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE, "100");
        config.setProperty(SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT, "1000");
        config.getMapConfig(MAP_NAME).setBackupCount(MAP_BACKUP_COUNT);
        return config;
    }

    @Test
    public void ensure_all_map_replicas_empty_after_entries_expired() {
        Runnable[] runnables = new Runnable[DRIVER_COUNT];
        for (int i = 0; i < DRIVER_COUNT; i++) {
            final HazelcastInstance driver = bounceMemberRule.getNextTestDriver();
            runnables[i] = new Runnable() {
                @Override
                public void run() {
                    assertTrueEventually(new AssertTask() {
                        @Override
                        public void run() throws Exception {
                            assertEquals(0, entryCountInRecordStore(driver, MAP_NAME));
                        }
                    });
                }
            };
        }

        bounceMemberRule.testRepeatedly(runnables, TEST_DURATION_SECONDS);
    }

    public static int entryCountInRecordStore(HazelcastInstance node, String mapName) {
        int size = 0;
        NodeEngineImpl nodeEngine = getNode(node).getNodeEngine();
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            RecordStore recordStore = mapServiceContext.getExistingRecordStore(i, mapName);
            if (recordStore == null) {
                continue;
            }

            size += recordStore.size();
        }
        return size;
    }
}
