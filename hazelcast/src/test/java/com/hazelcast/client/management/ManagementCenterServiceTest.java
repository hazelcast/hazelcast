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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static com.hazelcast.cluster.ClusterState.IN_TRANSITION;
import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.config.MapConfig.DEFAULT_MAX_IDLE_SECONDS;
import static com.hazelcast.config.MapConfig.DEFAULT_TTL_SECONDS;
import static com.hazelcast.config.MaxSizeConfig.DEFAULT_MAX_SIZE;
import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_NODE;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ManagementCenterServiceTest extends HazelcastTestSupport {
    private static final int NODE_COUNT = 3;

    private TestHazelcastFactory factory;
    private ManagementCenterService managementCenterService;
    private HazelcastInstance[] hazelcastInstances = new HazelcastInstance[NODE_COUNT];
    private Member[] members;

    @Before
    public void setUp() {
        factory = new TestHazelcastFactory(NODE_COUNT);
        System.arraycopy(factory.newInstances(getConfig(), NODE_COUNT - 1), 0, hazelcastInstances,
                0, NODE_COUNT - 1);
        hazelcastInstances[NODE_COUNT - 1] = factory.newHazelcastInstance(getConfig().setLiteMember(true));

        members = Arrays.stream(hazelcastInstances)
                .map(instance -> instance.getCluster().getLocalMember())
                .toArray(Member[]::new);

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

        resolve(managementCenterService.changeClusterState(PASSIVE));

        assertClusterState(PASSIVE, hazelcastInstances);
    }

    @Test(expected = IllegalArgumentException.class)
    public void changeClusterState_exception() throws Throwable {
        assertTrueEventually(
                () -> assertEquals(ACTIVE, hazelcastInstances[0].getCluster().getClusterState()));
        waitClusterForSafeState(hazelcastInstances[0]);

        try {
            resolve(managementCenterService.changeClusterState(IN_TRANSITION));
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    @Test
    public void getMapConfig_randomMember() throws Exception {
        MCMapConfig mapConfig = resolve(managementCenterService.getMapConfig("map-1"));
        assertEquals(1, mapConfig.getBackupCount());
    }

    @Test
    public void updateMapConfig() throws Exception {
        hazelcastInstances[0].getMap("map-1").put(1, 1);

        UpdateMapConfigParameters parameters = new UpdateMapConfigParameters(
                "map-1", 27, 29, EvictionPolicy.LRU, false, 35, PER_NODE);
        resolve(managementCenterService.updateMapConfig(members[0], parameters));

        MCMapConfig retrievedConfig1 = managementCenterService.getMapConfig(members[0], "map-1").get();
        assertEquals(27, retrievedConfig1.getTimeToLiveSeconds());
        assertEquals(29, retrievedConfig1.getMaxIdleSeconds());
        assertEquals(35, retrievedConfig1.getMaxSize());
        assertEquals(PER_NODE, retrievedConfig1.getMaxSizePolicy());

        MCMapConfig retrievedConfig2 = managementCenterService.getMapConfig(members[1], "map-1").get();
        assertEquals(DEFAULT_TTL_SECONDS, retrievedConfig2.getTimeToLiveSeconds());
        assertEquals(DEFAULT_MAX_IDLE_SECONDS, retrievedConfig2.getMaxIdleSeconds());
        assertEquals(DEFAULT_MAX_SIZE, retrievedConfig2.getMaxSize());
    }

    @Test
    public void getMemberConfig() throws Exception {
        String configXml1 = managementCenterService.getMemberConfig(members[0])
                .get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        String configXml2 = managementCenterService.getMemberConfig(members[1])
                .get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        assertEquals(configXml1, configXml2);
        assertContains(configXml1, "<cluster-name>dev</cluster-name>");
    }

    @Test
    public void runGc() throws Exception {
        resolve(managementCenterService.runGc(members[0]));
    }

    @Test
    public void getThreadDump() throws Exception {
        String threadDump = resolve(managementCenterService.getThreadDump(members[0], false));
        assertContains(threadDump, hazelcastInstances[0].getName());

        threadDump = resolve(managementCenterService.getThreadDump(members[1], false));
        assertContains(threadDump, hazelcastInstances[1].getName());

        threadDump = resolve(managementCenterService.getThreadDump(members[1], true));
        assertContains(threadDump, "Deadlocked thread dump");
    }

    @Test
    public void shutdownMember() {
        assertTrue(hazelcastInstances[0].getLifecycleService().isRunning());

        managementCenterService.shutdownMember(members[0]);

        assertTrueEventually(() -> assertFalse(hazelcastInstances[0].getLifecycleService().isRunning()));
    }

    @Test
    public void promoteMember() throws Exception {
        assertTrue(members[2].isLiteMember());

        resolve(managementCenterService.promoteLiteMember(members[2]));

        assertFalse(hazelcastInstances[2].getCluster().getLocalMember().isLiteMember());
    }

    @Test
    public void promoteMember_notLiteMember() {
        assertFalse(members[0].isLiteMember());

        assertThrows(IllegalStateException.class, () -> {
            try {
                resolve(managementCenterService.promoteLiteMember(members[0]));
            } catch (Exception e) {
                //noinspection ThrowableNotThrown
                rethrow(e);
            }
        });

        assertFalse(hazelcastInstances[0].getCluster().getLocalMember().isLiteMember());
    }

    @Test
    public void getSystemProperties() throws Exception {
        Map<String, String> entries = resolve(managementCenterService.getSystemProperties(members[0]));
        assertTrue(entries.containsKey("user.dir"));
        assertNotNull(entries.get("user.dir"));
        assertFalse(entries.get("user.dir").isEmpty());
    }

    private <T> T resolve(CompletableFuture<T> future) throws Exception {
        return future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
    }
}
