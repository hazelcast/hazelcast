package com.hazelcast.cp.internal;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.operation.GetLeadershipGroupsOp;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CPGroupRebalanceTest extends HazelcastRaftTestSupport {

    @ClassRule
    public static final OverridePropertyRule raftLeadershipRebalanceRule
            = OverridePropertyRule.set("hazelcast.raft.leadership.rebalance.period", String.valueOf(Integer.MAX_VALUE));

    @Test
    public void test() throws Exception {
        HazelcastInstance[] instances = newInstances(7, 3, 0);
        waitUntilCPDiscoveryCompleted(instances);

        RaftInvocationManager invocationManager = getRaftService(instances[0]).getInvocationManager();

        Collection<CPGroupId> groupIds = new ArrayList<CPGroupId>();
        for (int i = 0; i < 77; i++) {
            RaftGroupId groupId = invocationManager.createRaftGroup("group-" + i).join();
            groupIds.add(groupId);
        }

        HazelcastInstance metadataLeader = getLeaderInstance(instances, getMetadataGroupId(instances[0]));
        Collection<CPMember> cpMembers = metadataLeader.getCPSubsystem().getCPSubsystemManagementService().getCPMembers().get();

        for (CPGroupId groupId : groupIds) {
            // await leader election
            getLeaderInstance(instances, groupId);
        }

        Map<CPMember, Collection<CPGroupId>> leadershipsMap = getLeadershipsMap(metadataLeader, cpMembers);
        System.err.println("====== LEADERSHIPS ========");
        for (Entry<CPMember, Collection<CPGroupId>> entry : leadershipsMap.entrySet()) {
            System.err.println(entry.getKey() + " => " + entry.getValue().size());
        }

        getRaftService(metadataLeader).getMetadataGroupManager().rebalanceGroupLeaderships();

        leadershipsMap = getLeadershipsMap(metadataLeader, cpMembers);
        System.err.println("====== LEADERSHIPS ========");
        for (Entry<CPMember, Collection<CPGroupId>> entry : leadershipsMap.entrySet()) {
            System.err.println(entry.getKey() + " => " + entry.getValue().size());
        }

        getRaftService(metadataLeader).getMetadataGroupManager().rebalanceGroupLeaderships();

        System.err.println("====== LEADERSHIPS ========");
        leadershipsMap = getLeadershipsMap(metadataLeader, cpMembers);
        for (Entry<CPMember, Collection<CPGroupId>> entry : leadershipsMap.entrySet()) {
            System.err.println(entry.getKey() + " => " + entry.getValue().size());
        }
    }

    private Map<CPMember, Collection<CPGroupId>> getLeadershipsMap(HazelcastInstance instance,
            Collection<CPMember> members) {
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
