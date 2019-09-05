package com.hazelcast.cp.internal;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.operation.GetLeadershipGroupsOp;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
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

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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
        HazelcastInstance[] instances = newInstances(cpMemberCount, 3, 0);
        waitUntilCPDiscoveryCompleted(instances);

        final int leadershipsPerMember = 11;
        final int extraGroups = 3;
        final int groupCount = cpMemberCount * leadershipsPerMember + extraGroups;

        createRaftGroups(instances, groupCount);

        HazelcastInstance metadataLeader = getLeaderInstance(instances, getMetadataGroupId(instances[0]));
        Collection<CPMember> cpMembers = metadataLeader.getCPSubsystem().getCPSubsystemManagementService().getCPMembers().get();

        System.err.println(leadershipsString(getLeadershipsMap(metadataLeader, cpMembers)));

        rebalanceLeaderships(metadataLeader);

        Map<CPMember, Collection<CPGroupId>> leadershipsMap = getLeadershipsMap(metadataLeader, cpMembers);

        System.err.println(leadershipsString(leadershipsMap));

        for (Entry<CPMember, Collection<CPGroupId>> entry : leadershipsMap.entrySet()) {
            int count = entry.getValue().size();
            assertBetween(leadershipsString(leadershipsMap), count, leadershipsPerMember - 1, leadershipsPerMember + extraGroups);
//            assertThat(leadershipsString(leadershipsMap), count,
//                    isOneOf(leadershipsPerMember - 1, leadershipsPerMember, leadershipsPerMember + 1));
        }
    }

    private void rebalanceLeaderships(HazelcastInstance metadataLeader) {
        getRaftService(metadataLeader).getMetadataGroupManager().rebalanceGroupLeaderships();
    }

    private void createRaftGroups(HazelcastInstance[] instances, int groupCount) {
        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);

        Collection<CPGroupId> groupIds = new ArrayList<CPGroupId>(groupCount);
        Collection<InternalCompletableFuture<RaftGroupId>> futures
                = new ArrayList<InternalCompletableFuture<RaftGroupId>>(groupCount);

        for (int i = 0; i < groupCount; i++) {
            InternalCompletableFuture<RaftGroupId> f = invocationManager.createRaftGroup("group-" + i);
            futures.add(f);
        }
        for (InternalCompletableFuture<RaftGroupId> future : futures) {
            RaftGroupId groupId = future.join();
            groupIds.add(groupId);
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
        InternalOperationService operationService = getOperationService(instance);
        Map<CPMember, Collection<CPGroupId>> leaderships = new HashMap<CPMember, Collection<CPGroupId>>();

        for (CPMember member : members) {
            Collection<CPGroupId> groups =
                    operationService.<Collection<CPGroupId>>invokeOnTarget(null, new GetLeadershipGroupsOp(),
                            member.getAddress()).join();
            leaderships.put(member, groups);
        }
        return leaderships;
    }
}
