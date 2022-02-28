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

package com.hazelcast.cp.internal;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.operation.GetLeadedGroupsOp;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.cp.internal.RaftGroupMembershipManager.LEADERSHIP_BALANCE_TASK_PERIOD;
import static com.hazelcast.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;

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
        int groupSize = 5;
        int leadershipsPerMember = 10;
        int groupCount = groupSize * leadershipsPerMember;

        HazelcastInstance[] instances = newInstances(groupSize, groupSize, 0);
        waitUntilCPDiscoveryCompleted(instances);

        Collection<CPGroupId> groupIds = new ArrayList<>(groupCount);
        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        for (int i = 0; i < groupCount; i++) {
            RaftGroupId groupId = invocationManager.createRaftGroup("group-" + i).joinInternal();
            groupIds.add(groupId);
        }

        HazelcastInstance metadataLeader = getLeaderInstance(instances, getMetadataGroupId(instances[0]));
        Collection<CPMember> cpMembers =
                metadataLeader.getCPSubsystem().getCPSubsystemManagementService().getCPMembers().toCompletableFuture().get();

        // Assert eventually since during the test
        // a long pause can cause leadership change unexpectedly.
        assertTrueEventually(() -> {
            // Wait for leader election all groups
            for (CPGroupId groupId : groupIds) {
                waitAllForLeaderElection(instances, groupId);
            }

            rebalanceLeaderships(metadataLeader);

            Map<CPMember, Collection<CPGroupId>> leadershipsMap = getLeadershipsMap(metadataLeader, cpMembers);
            for (Entry<CPMember, Collection<CPGroupId>> entry : leadershipsMap.entrySet()) {
                int count = entry.getValue().size();
                assertEquals(leadershipsString(leadershipsMap), leadershipsPerMember, count);
            }
        });
    }

    private void rebalanceLeaderships(HazelcastInstance metadataLeader) {
        getRaftService(metadataLeader).getMetadataGroupManager().rebalanceGroupLeaderships();
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
                    operationService.<Collection<CPGroupId>>invokeOnTarget(null, new GetLeadedGroupsOp(), member.getAddress())
                            .join();
            leaderships.put(member, groups);
        }
        return leaderships;
    }
}
