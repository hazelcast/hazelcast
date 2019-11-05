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

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.management.MCMapConfig;
import com.hazelcast.client.impl.management.ManagementCenterService;
import com.hazelcast.client.impl.management.UpdateMapConfigParameters;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.EvictionPolicy;
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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static com.hazelcast.cluster.ClusterState.IN_TRANSITION;
import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.config.MapConfig.DEFAULT_MAX_IDLE_SECONDS;
import static com.hazelcast.config.MapConfig.DEFAULT_MAX_SIZE;
import static com.hazelcast.config.MapConfig.DEFAULT_TTL_SECONDS;
import static com.hazelcast.config.MaxSizePolicy.PER_PARTITION;
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

        CompletableFuture<Void> future = managementCenterService.changeClusterState(PASSIVE);
        future.get();

        assertClusterState(PASSIVE, hazelcastInstances);
    }

    @Test(expected = IllegalArgumentException.class)
    public void changeClusterState_exception() throws Throwable {
        assertTrueEventually(
                () -> assertEquals(ACTIVE, hazelcastInstances[0].getCluster().getClusterState()));
        waitClusterForSafeState(hazelcastInstances[0]);

        CompletableFuture<Void> future = managementCenterService.changeClusterState(IN_TRANSITION);
        try {
            future.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test
    public void getMapConfig_randomMember() throws Exception {
        CompletableFuture<MCMapConfig> future = managementCenterService.getMapConfig("map-1");
        MCMapConfig mapConfig = future.get();
        assertEquals(1, mapConfig.getBackupCount());
    }

    @Test
    public void updateMapConfig() {
        hazelcastInstances[0].getMap("map-1").put(1, 1);

        Member member1 = hazelcastInstances[0].getCluster().getLocalMember();
        Member member2 = hazelcastInstances[1].getCluster().getLocalMember();

        UpdateMapConfigParameters parameters = new UpdateMapConfigParameters(
                "map-1", 27, 29, EvictionPolicy.LFU,
                false, 1000, PER_PARTITION);
        managementCenterService.updateMapConfig(member1, parameters);

        assertTrueEventually(() -> {
            MCMapConfig retrievedConfig1 = managementCenterService.getMapConfig(member1, "map-1").get();
            assertEquals(27, retrievedConfig1.getTimeToLiveSeconds());
            assertEquals(29, retrievedConfig1.getMaxIdleSeconds());
            assertEquals(1000, retrievedConfig1.getMaxSize());
            assertEquals(PER_PARTITION, retrievedConfig1.getMaxSizePolicy());
        });

        assertTrueEventually(() -> {
            MCMapConfig retrievedConfig2 = managementCenterService.getMapConfig(member2, "map-1").get();
            assertEquals(DEFAULT_TTL_SECONDS, retrievedConfig2.getTimeToLiveSeconds());
            assertEquals(DEFAULT_MAX_IDLE_SECONDS, retrievedConfig2.getMaxIdleSeconds());
            assertEquals(DEFAULT_MAX_SIZE, retrievedConfig2.getMaxSize());
        }, 10);

    }
}
