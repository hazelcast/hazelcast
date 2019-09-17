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

package com.hazelcast.client.management;

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.management.ManagementCenterService;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ManagementCenterServiceTest extends HazelcastTestSupport {
    private static final int NODE_COUNT = 3;

    private TestHazelcastFactory factory;
    private ManagementCenterService managementCenterService;
    private HazelcastInstance[] hazelcastInstances;

    @Before
    public void setUp() {
        factory = new TestHazelcastFactory(NODE_COUNT);
        hazelcastInstances = factory.newInstances(getConfig(), NODE_COUNT);

        HazelcastInstance client = factory.newHazelcastClient();
        managementCenterService = ((HazelcastClientProxy) client).client.getManagementCenterService();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void changeClusterState() throws Exception {
        assertTrueEventually(
                () -> assertEquals(ACTIVE, hazelcastInstances[0].getCluster().getClusterState()));
        waitClusterForSafeState(hazelcastInstances[0]);

        ClientDelegatingFuture<Void> future = managementCenterService.changeClusterState(PASSIVE);
        future.get();

        assertClusterState(PASSIVE, hazelcastInstances);
    }

    @Test
    public void getMapConfig() throws Exception {
        ClientDelegatingFuture<MapConfig> future = managementCenterService.getMapConfig("map-1");
        MapConfig mapConfig = future.get();
        assertEquals("default", mapConfig.getName());
    }

    @Test
    public void updateMapConfig() {
        hazelcastInstances[0].getMap("map-1").put(1, 1);
        MapConfig mapConfig = new MapConfig("map-1");
        mapConfig.setMaxIdleSeconds(27);
        managementCenterService.updateMapConfig("map-1", mapConfig);

        assertTrueEventually(() -> {
            MapConfig retrievedConfig = managementCenterService.getMapConfig("map-1").get();
            assertEquals(27, retrievedConfig.getMaxIdleSeconds());
        });
    }
}
