/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.config.MaxSizePolicy.PER_NODE;
import static com.hazelcast.spi.properties.ClusterProperty.MC_TRUSTED_INTERFACES;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.security.AccessControlException;
import java.util.concurrent.CompletableFuture;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.management.MCWanBatchPublisherConfig;
import com.hazelcast.client.impl.management.ManagementCenterService;
import com.hazelcast.client.impl.management.UpdateMapConfigParameters;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanQueueFullBehavior;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.management.dto.ClientBwListDTO;
import com.hazelcast.internal.management.dto.ClientBwListEntryDTO;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import com.hazelcast.wan.WanPublisherState;

/**
 * This class tests Management Center tasks protection on the client protocol. Most of the tests check that client's source
 * address which is not listed in the {@link ManagementCenterConfig#getTrustedInterfaces()} set prevents the client to run
 * protected tasks.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class ManagementCenterTrustedInterfacesTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void configureInterfacesByProperties_trustedAllowed() throws Exception {
        Config config = new Config();
        config.setProperty(MC_TRUSTED_INTERFACES.getName(), "192.168.1.*");
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        assertTrueEventually(() -> assertEquals(ACTIVE, hz.getCluster().getClusterState()));

        HazelcastInstance client = factory.newHazelcastClient(null, "192.168.1.111");
        ManagementCenterService managementCenterService = ((HazelcastClientProxy) client).client.getManagementCenterService();
        resolve(managementCenterService.changeClusterState(PASSIVE));

        assertClusterState(PASSIVE, hz);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void configureInterfacesByProperties_untrustedNotAllowed() throws Exception {
        Config config = new Config();
        config.setProperty(MC_TRUSTED_INTERFACES.getName(), "192.168.1.*");
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        assertTrueEventually(() -> assertEquals(ACTIVE, hz.getCluster().getClusterState()));

        HazelcastInstance client = factory.newHazelcastClient(null, "192.168.2.111");
        ManagementCenterService managementCenterService = ((HazelcastClientProxy) client).client.getManagementCenterService();
        expected.expectCause(isA(AccessControlException.class));
        resolve(managementCenterService.changeClusterState(PASSIVE));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void onlyOneConfigMethodAllowed() throws Exception {
        Config config = new Config();
        config.setProperty(MC_TRUSTED_INTERFACES.getName(), "192.168.1.*");
        config.getManagementCenterConfig().addTrustedInterface("192.168.1.111");
        expected.expect(InvalidConfigurationException.class);
        factory.newHazelcastInstance(config);
    }

    @Test
    public void changeClusterState_defaultAllowed() throws Exception {
        HazelcastInstance hz = factory.newHazelcastInstance();
        assertTrueEventually(() -> assertEquals(ACTIVE, hz.getCluster().getClusterState()));

        HazelcastInstance client = factory.newHazelcastClient();
        ManagementCenterService managementCenterService = ((HazelcastClientProxy) client).client.getManagementCenterService();

        resolve(managementCenterService.changeClusterState(PASSIVE));

        assertClusterState(PASSIVE, hz);
    }

    @Test
    public void changeClusterState_trustedAllowed() throws Exception {
        Config config = new Config();
        config.getManagementCenterConfig().addTrustedInterface("192.168.1.*");
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        assertTrueEventually(() -> assertEquals(ACTIVE, hz.getCluster().getClusterState()));

        HazelcastInstance client = factory.newHazelcastClient(null, "192.168.1.111");
        ManagementCenterService managementCenterService = ((HazelcastClientProxy) client).client.getManagementCenterService();
        resolve(managementCenterService.changeClusterState(PASSIVE));

        assertClusterState(PASSIVE, hz);
    }

    @Test
    public void changeClusterState_untrustedNotAllowed() throws Exception {
        Config config = new Config();
        config.getManagementCenterConfig().addTrustedInterface("192.168.1.111");
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        assertTrueEventually(() -> assertEquals(ACTIVE, hz.getCluster().getClusterState()));

        HazelcastInstance client = factory.newHazelcastClient(null, "192.168.2.111");
        ManagementCenterService managementCenterService = ((HazelcastClientProxy) client).client.getManagementCenterService();
        expected.expectCause(isA(AccessControlException.class));
        resolve(managementCenterService.changeClusterState(PASSIVE));
    }

    @Test
    public void runGc_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.runGc(scenario.member));
    }

    @Test
    public void getMapConfig_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.getMapConfig("map-1"));
    }

    @Test
    public void updateMapConfig_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        scenario.hz.getMap("map-1").put(1, 1);

        UpdateMapConfigParameters parameters = new UpdateMapConfigParameters("map-1", 27, 29, EvictionPolicy.LRU, false, 35,
                PER_NODE);
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.updateMapConfig(scenario.member, parameters));
    }

    @Test
    public void getMemberConfig_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.getMemberConfig(scenario.member));
    }

    @Test
    public void getThreadDump_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.getThreadDump(scenario.member, false));
    }

    @Test
    public void shutdownMember_untrustedNotAllowed() {
        Scenario scenario = createUntrustedTestScenario();
        scenario.managementCenterService.shutdownMember(scenario.member);
        assertTrue(scenario.hz.getLifecycleService().isRunning());
    }

    @Test
    public void promoteMember_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.promoteLiteMember(scenario.member));
    }

    @Test
    public void getSystemProperties_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.getSystemProperties(scenario.member));
    }

    @Test
    public void getTimedMemberState_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.getTimedMemberState(scenario.member));
    }

    @Test
    public void getClusterMetadata_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.getClusterMetadata(scenario.member));
    }

    @Test
    public void shutdownCluster_untrustedNotAllowed() {
        Scenario scenario = createUntrustedTestScenario();
        scenario.managementCenterService.shutdownCluster();
        assertTrue(scenario.hz.getLifecycleService().isRunning());
    }

    @Test
    public void changeClusterVersion_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService
                .changeClusterVersion(Version.of(BuildInfoProvider.getBuildInfo().getVersion())));
    }

    @Test
    public void matchMCConfig_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.matchMCConfig(scenario.member, "eTag"));
    }

    @Test
    public void applyMCConfig_untrustedNotAllowed() throws Exception {
        ClientBwListDTO bwListDTO = new ClientBwListDTO(ClientBwListDTO.Mode.BLACKLIST,
                singletonList(new ClientBwListEntryDTO(ClientBwListEntryDTO.Type.INSTANCE_NAME, "test-name")));
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.applyMCConfig(scenario.member, "eTag", bwListDTO));
    }

    @Test
    public void runScript_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.runScript(scenario.member, "javascript", "'hello world';"));
    }

    @Test
    public void runConsoleCommand_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.runConsoleCommand(scenario.member, null, "m.size"));
    }

    @Test
    public void pollMCEvents_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.pollMCEvents(scenario.member));
    }

    @Test
    public void getCPMembers_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.getCPMembers());
    }

    @Test
    public void promoteToCPMember_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.promoteToCPMember(scenario.member));
    }

    @Test
    public void removeCPMember_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.removeCPMember(scenario.member.getUuid()));
    }

    @Test
    public void resetCPSubsystem_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.resetCPSubsystem());
    }

    @Test
    public void addWanReplicationConfig_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        MCWanBatchPublisherConfig config = new MCWanBatchPublisherConfig();
        config.setAckType(WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE);
        config.setQueueFullBehaviour(WanQueueFullBehavior.THROW_EXCEPTION);
        config.setName("test");
        config.setTargetCluster("test");
        config.setEndpoints("test");
        resolve(scenario.managementCenterService.addWanReplicationConfig(config));
    }

    @Test
    public void changeWanReplicationState_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.changeWanReplicationState(scenario.member, "test", "publisherId",
                WanPublisherState.STOPPED));
    }

    @Test
    public void checkWanConsistency_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.checkWanConsistency("test", "publisherId", "mymap"));
    }

    @Test
    public void clearWanQueues_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.clearWanQueues(scenario.member, "wanRepName", "publisherId"));
    }

    @Test
    public void wanSyncAllMaps_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.wanSyncAllMaps("rep", "pubId"));
    }

    @Test
    public void wanSyncMap_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.wanSyncMap("wanRepName", "pubId", "map"));
    }

    @Test
    public void interruptHotRestartBackup_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.interruptHotRestartBackup());
    }

    @Test
    public void readMetricsAsync_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.readMetricsAsync(scenario.member, 0L));
    }

    @Test
    public void triggerForceStart_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.triggerForceStart());
    }

    @Test
    public void triggerHotRestartBackup_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.triggerHotRestartBackup());
    }

    @Test
    public void triggerPartialStart_untrustedNotAllowed() throws Exception {
        Scenario scenario = createUntrustedTestScenario();
        expected.expectCause(isA(AccessControlException.class));
        resolve(scenario.managementCenterService.triggerPartialStart());
    }

    private static <T> T resolve(CompletableFuture<T> future) throws Exception {
        return future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
    }

    private Scenario createUntrustedTestScenario() {
        return new Scenario(factory);
    }

    static class Scenario {

        final HazelcastInstance hz;
        final HazelcastInstance client;
        final ManagementCenterService managementCenterService;
        final Member member;

        Scenario(TestHazelcastFactory factory) {
            Config config = new Config();
            config.getManagementCenterConfig().addTrustedInterface("192.168.1.*");
            hz = factory.newHazelcastInstance(config);
            client = factory.newHazelcastClient(null, "192.168.2.111");
            member = client.getCluster().getMembers().iterator().next();
            managementCenterService = ((HazelcastClientProxy) client).client.getManagementCenterService();
        }
    }
}
