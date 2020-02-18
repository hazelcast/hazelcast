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

import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.management.ManagementCenterService;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.CPMemberInfo;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.internal.management.dto.CPMemberDTO;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.hazelcast.cp.CPGroup.METADATA_CP_GROUP_NAME;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ManagementCenterServiceCPOperationsTest extends HazelcastRaftTestSupport {

    private ManagementCenterService managementCenterService;

    @Override
    @Before
    public void init() {
        factory = new TestHazelcastFactory();
    }

    @Test
    public void getCPMembers() throws Exception {
        HazelcastInstance[] cpInstances = newInstances(3);
        initManagementCenterService();

        CPMember expectedCPMember = cpInstances[0].getCPSubsystem().getLocalCPMember();

        List<CPMemberDTO> cpMembers = resolve(managementCenterService.getCPMembers());
        assertEquals(3, cpMembers.size());
        List<UUID> cpUuids = cpMembers.stream().map(CPMemberDTO::getCPUuid).collect(Collectors.toList());
        assertContains(cpUuids, expectedCPMember.getUuid());
        List<UUID> uuids = cpMembers.stream().map(CPMemberDTO::getUuid).collect(Collectors.toList());
        assertContains(uuids, cpInstances[0].getCluster().getLocalMember().getUuid());
    }

    @Test
    public void promoteToCPMember() throws Exception {
        HazelcastInstance[] instances = newInstances(3, 3, 1);
        initManagementCenterService();

        HazelcastInstance instance = instances[instances.length - 1];
        resolve(managementCenterService.promoteToCPMember(instance.getCluster().getLocalMember()));

        assertNotNull(instance.getCPSubsystem().getLocalCPMember());
    }

    @Test
    public void removeCPMember() throws Exception {
        HazelcastInstance[] instances = newInstances(3);
        initManagementCenterService();

        Member member = instances[0].getCluster().getLocalMember();
        instances[0].getLifecycleService().terminate();

        assertClusterSizeEventually(2, instances[1]);

        CPMemberInfo removedEndpoint = new CPMemberInfo(member);
        resolve(managementCenterService.removeCPMember(removedEndpoint.getUuid()));

        CPGroup metadataGroup = instances[1].getCPSubsystem().getCPSubsystemManagementService()
                                            .getCPGroup(METADATA_CP_GROUP_NAME)
                                            .toCompletableFuture()
                                            .get();
        assertEquals(2, metadataGroup.members().size());
    }

    @Test
    public void resetCPSubsystem() throws Exception {
        HazelcastInstance[] instances = newInstances(3);
        RaftGroupId groupId = getRaftInvocationManager(instances[0]).createRaftGroup(CPGroup.DEFAULT_GROUP_NAME).get();
        initManagementCenterService();

        instances[0].getLifecycleService().terminate();
        assertClusterSizeEventually(2, instances[1], instances[2]);

        Config config = createConfig(3, 3);
        instances[0] = factory.newHazelcastInstance(config);
        assertClusterSizeEventually(3, instances[0]);

        resolve(managementCenterService.resetCPSubsystem());
        waitUntilCPDiscoveryCompleted(instances);

        RaftGroupId newGroupId = getRaftInvocationManager(instances[0]).createRaftGroup(CPGroup.DEFAULT_GROUP_NAME).get();
        assertThat(newGroupId.getSeed(), greaterThan(groupId.getSeed()));
    }

    private void initManagementCenterService() {
        HazelcastInstance client = ((TestHazelcastFactory) factory).newHazelcastClient();
        managementCenterService = ((HazelcastClientProxy) client).client.getManagementCenterService();
    }

    private static <T> T resolve(CompletableFuture<T> future) throws Exception {
        return future.get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
    }

    public static RaftService getRaftService(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(RaftService.SERVICE_NAME);
    }
}
