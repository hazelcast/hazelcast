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

package com.hazelcast.cp.internal;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.operation.GetLeadedGroupsOp;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raftop.metadata.CreateRaftGroupOp;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.cp.internal.MetadataRaftGroupManager.INITIAL_METADATA_GROUP_ID;
import static com.hazelcast.cp.internal.RaftGroupMembershipManager.LEADERSHIP_BALANCE_TASK_PERIOD;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPGroupRebalanceTest extends HazelcastRaftTestSupport {

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.setProperty(LEADERSHIP_BALANCE_TASK_PERIOD.getName(), String.valueOf(Integer.MAX_VALUE));
        return config;
    }

    @Test
    public void test() throws Exception {
        int cpMemberCount = 7;
        int groupSize = 3;
        HazelcastInstance[] instances = newInstances(cpMemberCount, groupSize, 0);
        waitUntilCPDiscoveryCompleted(instances);

        int leadershipsPerMember = 11;
        int extraGroups = 3;
        int groupCount = cpMemberCount * leadershipsPerMember + extraGroups;

        createRaftGroups(instances, groupSize, groupCount);

        HazelcastInstance metadataLeader = getLeaderInstance(instances, getMetadataGroupId(instances[0]));
        Collection<CPMember> cpMembers = metadataLeader.getCPSubsystem().getCPSubsystemManagementService().getCPMembers()
                                                       .toCompletableFuture().get();

        rebalanceLeaderships(metadataLeader);

        Map<CPMember, Collection<CPGroupId>> leadershipsMap = getLeadershipsMap(metadataLeader, cpMembers);

        for (Entry<CPMember, Collection<CPGroupId>> entry : leadershipsMap.entrySet()) {
            int count = entry.getValue().size();
            assertBetween(leadershipsString(leadershipsMap), count, leadershipsPerMember - 1, leadershipsPerMember + extraGroups);
        }
    }

    private void rebalanceLeaderships(HazelcastInstance metadataLeader) {
        getRaftService(metadataLeader).getMetadataGroupManager().rebalanceGroupLeaderships();
    }

    private void createRaftGroups(HazelcastInstance[] instances, int groupSize, int groupCount) {
        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);

        RaftEndpoint[] endpoints = Arrays.stream(instances)
                                         .map(instance -> instance.getCPSubsystem().getLocalCPMember())
                                         .map(cpMember -> ((CPMemberInfo) cpMember).toRaftEndpoint())
                                         .toArray(RaftEndpoint[]::new);
        Collection<CPGroupId> groupIds = new ArrayList<>(groupCount);
        Collection<InternalCompletableFuture<CPGroupSummary>> futures = new ArrayList<>(groupCount);

        for (int i = 0; i < groupCount; i++) {
            List<RaftEndpoint> groupMembers = new ArrayList<>(groupSize);
            for (int j = 0; j < groupSize; j++) {
                groupMembers.add(endpoints[(i + j) % endpoints.length]);
            }

            RaftOp op = new CreateRaftGroupOp("group-" + i, groupMembers);
            InternalCompletableFuture<CPGroupSummary> f = invocationManager.invoke(INITIAL_METADATA_GROUP_ID, op);
            futures.add(f);
        }

        for (InternalCompletableFuture<CPGroupSummary> future : futures) {
            CPGroupSummary group = future.join();
            invocationManager.triggerRaftNodeCreation(group);
            groupIds.add(group.id());
        }

        for (CPGroupId groupId : groupIds) {
            // await leader election
            getLeaderInstance(instances, groupId);
        }
    }

    private String leadershipsString(Map<CPMember, Collection<CPGroupId>> leadershipsMap) {
        StringBuilder s = new StringBuilder("====== LEADERSHIPS ======\n");
        for (Entry<CPMember, Collection<CPGroupId>> entry : leadershipsMap.entrySet()) {
            s.append(entry.getKey()).append(" => ").append(entry.getValue().size()).append('\n');
        }
        return s.toString();
    }

    private Map<CPMember, Collection<CPGroupId>> getLeadershipsMap(HazelcastInstance instance, Collection<CPMember> members) {
        OperationServiceImpl operationService = getOperationService(instance);
        Map<CPMember, Collection<CPGroupId>> leaderships = new HashMap<>();

        for (CPMember member : members) {
            Collection<CPGroupId> groups =
                    operationService.<Collection<CPGroupId>>invokeOnTarget(null, new GetLeadedGroupsOp(),
                            member.getAddress()).join();
            leaderships.put(member, groups);
        }
        return leaderships;
    }
}
