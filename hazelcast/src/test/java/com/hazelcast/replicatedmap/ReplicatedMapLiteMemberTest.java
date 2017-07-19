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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReplicatedMapLiteMemberTest extends HazelcastTestSupport {

    @Test
    public void testLiteMembersWithReplicatedMap() {
        Config config = buildConfig(false);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);

        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        final HazelcastInstance lite = nodeFactory.newHazelcastInstance(buildConfig(true));

        ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");

        map1.put("key", "value");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(instance2.getReplicatedMap("default").containsKey("key"));
            }
        });

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                ReplicatedMapService service = getReplicatedMapService(lite);
                assertEquals(0, service.getAllReplicatedRecordStores("default").size());
            }
        }, 5);
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
        return nodeFactory.newHazelcastInstance(buildConfig(true));
    }

    private ReplicatedMapService getReplicatedMapService(HazelcastInstance lite) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(lite);
        return nodeEngine.getService(ReplicatedMapService.SERVICE_NAME);
    }

    private Config buildConfig(boolean liteMember) {
        Config config = new Config();
        config.setLiteMember(liteMember);
        return config;
    }
}
