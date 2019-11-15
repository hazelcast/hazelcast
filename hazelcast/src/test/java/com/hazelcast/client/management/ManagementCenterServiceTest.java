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
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.dto.ClientBwListDTO;
import com.hazelcast.internal.management.dto.ClientBwListEntryDTO;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static com.hazelcast.cluster.ClusterState.IN_TRANSITION;
import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.config.MapConfig.DEFAULT_MAX_IDLE_SECONDS;
import static com.hazelcast.config.MapConfig.DEFAULT_MAX_SIZE;
import static com.hazelcast.config.MapConfig.DEFAULT_TTL_SECONDS;
import static com.hazelcast.config.MaxSizePolicy.PER_NODE;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
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
        hazelcastInstances[0] = factory.newHazelcastInstance(getConfig());
        hazelcastInstances[1] = factory.newHazelcastInstance(getConfig().setLiteMember(true));
        Config config = getConfig();
        config.getManagementCenterConfig().setEnabled(true).setUrl("a");
        hazelcastInstances[2] = factory.newHazelcastInstance(config);

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
        assertContains(configXml1, "<cluster-name>dev</cluster-name>");
        assertContains(configXml2, "<cluster-name>dev</cluster-name>");
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
        assertTrue(members[1].isLiteMember());

        resolve(managementCenterService.promoteLiteMember(members[1]));

        assertFalse(hazelcastInstances[1].getCluster().getLocalMember().isLiteMember());
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
        assertThat(entries.get("user.dir"), not(isEmptyOrNullString()));
    }

    @Test
    public void testGetTimedMemberState() {
        assertTrueEventually(() -> {
            Optional<String> timedMemberStateJson = managementCenterService.getTimedMemberState(members[2])
                    .get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
            assertTrue(timedMemberStateJson.isPresent());
        });
    }

    @Test
    public void testGetTimedMemberState_empty() throws Exception {
        Optional<String> timedMemberStateJson = managementCenterService.getTimedMemberState(members[0])
                .get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        assertFalse(timedMemberStateJson.isPresent());
    }

    private <T> T resolve(CompletableFuture<T> future) throws Exception {
        return future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
    }

    @Test
    public void matchMCConfig() throws Exception {
        ClientBwListDTO bwListDTO = new ClientBwListDTO(ClientBwListDTO.Mode.DISABLED, emptyList());
        getMemberMCService(hazelcastInstances[0]).applyMCConfig("testETag", bwListDTO);

        assertTrue(managementCenterService.matchMCConfig(members[0], "testETag").get());
        assertFalse(managementCenterService.matchMCConfig(members[0], "wrongETag").get());
    }

    @Test
    public void applyMCConfig() throws Exception {
        assertNull(getMemberMCService(hazelcastInstances[0]).getLastMCConfigETag());

        ClientBwListDTO bwListDTO = new ClientBwListDTO(
                ClientBwListDTO.Mode.BLACKLIST,
                singletonList(new ClientBwListEntryDTO(ClientBwListEntryDTO.Type.INSTANCE_NAME, "test-name"))
        );
        managementCenterService.applyMCConfig(members[0], "testETag", bwListDTO).get();

        assertEquals("testETag", getMemberMCService(hazelcastInstances[0]).getLastMCConfigETag());
    }

    private static com.hazelcast.internal.management.ManagementCenterService getMemberMCService(HazelcastInstance instance) {
        return getNode(instance).getManagementCenterService();
    }
}
