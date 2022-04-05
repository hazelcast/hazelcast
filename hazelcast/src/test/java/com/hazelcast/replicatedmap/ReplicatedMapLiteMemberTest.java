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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapLiteMemberTest extends HazelcastTestSupport {

    private Config dataMemberConfig = buildConfig(false);
    private Config liteMemberConfig = buildConfig(true);

    @Test
    public void testLiteMembersWithReplicatedMap() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(dataMemberConfig);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(dataMemberConfig);
        final HazelcastInstance lite = nodeFactory.newHazelcastInstance(liteMemberConfig);

        ReplicatedMap<String, String> replicatedMap = instance1.getReplicatedMap("default");

        replicatedMap.put("key", "value");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(instance1.getReplicatedMap("default").containsKey("key"));
                assertTrue(instance2.getReplicatedMap("default").containsKey("key"));
            }
        });
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                ReplicatedMapService service = getReplicatedMapService(lite);
                assertEquals(0, service.getAllReplicatedRecordStores("default").size());
            }
        }, 3);
    }

    @Test
    public void testPromoteLiteMember() {
        String mapName = randomName();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(dataMemberConfig);

        ReplicatedMap<String, String> map = instance1.getReplicatedMap(mapName);
        map.put("key", "value");

        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(liteMemberConfig);
        instance2.getCluster().promoteLocalLiteMember();

        final ReplicatedMap<String, String> promotedMap = instance2.getReplicatedMap(mapName);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals("Expected the promoted lite member to retrieve a value from a ReplicatedMap",
                        "value", promotedMap.get("key"));
            }
        });
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testCreateReplicatedMapOnLiteMember() {
        HazelcastInstance lite = createSingleLiteMember();
        lite.getReplicatedMap("default");
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testCreateReplicatedStoreOnLiteMember() {
        HazelcastInstance lite = createSingleLiteMember();
        ReplicatedMapService service = getReplicatedMapService(lite);
        service.getReplicatedRecordStore("default", true, 1);
    }

    @Test(expected = ReplicatedMapCantBeCreatedOnLiteMemberException.class)
    public void testGetReplicatedStoreOnLiteMember() {
        HazelcastInstance lite = createSingleLiteMember();
        ReplicatedMapService service = getReplicatedMapService(lite);
        service.getReplicatedRecordStore("default", false, 1);
    }

    private HazelcastInstance createSingleLiteMember() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        return nodeFactory.newHazelcastInstance(liteMemberConfig);
    }

    private ReplicatedMapService getReplicatedMapService(HazelcastInstance instance) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        return nodeEngine.getService(SERVICE_NAME);
    }

    private Config buildConfig(boolean liteMember) {
        return smallInstanceConfig()
                .setLiteMember(liteMember);
    }
}
