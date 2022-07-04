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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionStateGenerator;
import com.hazelcast.internal.partition.ReadonlyInternalPartition;
import com.hazelcast.internal.partition.membergroup.ConfigMemberGroupFactory;
import com.hazelcast.internal.partition.membergroup.DefaultMemberGroup;
import com.hazelcast.internal.partition.membergroup.HostAwareMemberGroupFactory;
import com.hazelcast.internal.partition.membergroup.MemberGroupFactory;
import com.hazelcast.internal.partition.membergroup.SingleMemberGroupFactory;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.spi.partitiongroup.MemberGroup;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionStateGeneratorTest {

    private static final MemberVersion VERSION = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());
    private static final boolean PRINT_STATE = false;

    @Test
    public void testRandomPartitionGenerator() throws Exception {
        final MemberGroupFactory memberGroupFactory = new SingleMemberGroupFactory();
        test(memberGroupFactory);
    }

    //"random host groups may cause non-uniform distribution of partitions when node size go down significantly!")
    @Test
    public void testHostAwarePartitionStateGenerator() throws Exception {
        final HostAwareMemberGroupFactory memberGroupFactory = new HostAwareMemberGroupFactory();
        test(memberGroupFactory);
    }

    @Test
    public void testCustomPartitionStateGenerator() throws Exception {
        final MemberGroupFactory memberGroupFactory = new MemberGroupFactory() {
            public Collection<MemberGroup> createMemberGroups(Collection<? extends Member> members) {
                MemberGroup[] g = new MemberGroup[4];
                for (int i = 0; i < g.length; i++) {
                    g[i] = new DefaultMemberGroup();
                }
                for (Member member : members) {
                    Address address = member.getAddress();
                    if (even(address.getHost().hashCode()) && even(address.getPort())) {
                        g[0].addMember(member);
                    } else if (even(address.getHost().hashCode()) && !even(address.getPort())) {
                        g[1].addMember(member);
                    } else if (!even(address.getHost().hashCode()) && even(address.getPort())) {
                        g[2].addMember(member);
                    } else if (!even(address.getHost().hashCode()) && !even(address.getPort())) {
                        g[3].addMember(member);
                    }
                }
                List<MemberGroup> list = new LinkedList<MemberGroup>();
                for (MemberGroup memberGroup : g) {
                    if (memberGroup.size() > 0) {
                        list.add(memberGroup);
                    }
                }
                return list;
            }

            boolean even(int k) {
                return k % 2 == 0;
            }
        };
        test(memberGroupFactory);
    }

    @Test
    public void testConfigCustomPartitionStateGenerator() throws Exception {
        PartitionGroupConfig config = new PartitionGroupConfig();
        config.setEnabled(true);
        config.setGroupType(PartitionGroupConfig.MemberGroupType.CUSTOM);
        MemberGroupConfig mgCfg0 = new MemberGroupConfig();
        MemberGroupConfig mgCfg1 = new MemberGroupConfig();
        MemberGroupConfig mgCfg2 = new MemberGroupConfig();
        MemberGroupConfig mgCfg3 = new MemberGroupConfig();

        config.addMemberGroupConfig(mgCfg0);
        config.addMemberGroupConfig(mgCfg1);
        config.addMemberGroupConfig(mgCfg2);
        config.addMemberGroupConfig(mgCfg3);

        for (int k = 0; k < 3; k++) {
            for (int i = 0; i < 255; i++) {
                MemberGroupConfig mg;
                switch (i % 4) {
                    case 0:
                        mg = mgCfg0;
                        break;
                    case 1:
                        mg = mgCfg1;
                        break;
                    case 2:
                        mg = mgCfg2;
                        break;
                    case 3:
                        mg = mgCfg3;
                        break;
                    default:
                        throw new IllegalArgumentException();
                }
                mg.addInterface("10.10." + k + "." + i);
            }
        }

        test(new ConfigMemberGroupFactory(config.getMemberGroupConfigs()));
    }

    @Test
    public void testXmlPartitionGroupConfig() {
        Config config = new ClasspathXmlConfig("hazelcast-fullconfig.xml");
        PartitionGroupConfig partitionGroupConfig = config.getPartitionGroupConfig();
        assertTrue(partitionGroupConfig.isEnabled());
        assertEquals(PartitionGroupConfig.MemberGroupType.CUSTOM, partitionGroupConfig.getGroupType());
        assertEquals(2, partitionGroupConfig.getMemberGroupConfigs().size());
    }

    @Test
    public void testOnlyUnassignedArrangement() throws Exception {
        List<Member> memberList = createMembers(10, 1);
        MemberGroupFactory memberGroupFactory = new SingleMemberGroupFactory();
        Collection<MemberGroup> groups = memberGroupFactory.createMemberGroups(memberList);

        PartitionStateGenerator generator = new PartitionStateGeneratorImpl();
        PartitionReplica[][] state = generator.arrange(groups, emptyPartitionArray(100));

        // unassign some partitions entirely
        Collection<Integer> unassignedPartitions = new ArrayList<Integer>();
        for (int i = 0; i < state.length; i++) {
            if (i % 3 == 0) {
                state[i] = new PartitionReplica[InternalPartition.MAX_REPLICA_COUNT];
                unassignedPartitions.add(i);
            }
        }

        // unassign only backup replicas of some partitions
        for (int i = 0; i < state.length; i++) {
            if (i % 10 == 0) {
                Arrays.fill(state[i], 1, InternalPartition.MAX_REPLICA_COUNT, null);
            }
        }

        InternalPartition[] partitions = toPartitionArray(state);

        state = generator.arrange(groups, partitions, unassignedPartitions);

        for (int pid = 0; pid < state.length; pid++) {
            PartitionReplica[] addresses = state[pid];

            if (unassignedPartitions.contains(pid)) {
                for (PartitionReplica address : addresses) {
                    assertNotNull(address);
                }
            } else {
                InternalPartition partition = partitions[pid];
                for (int replicaIx = 0; replicaIx < InternalPartition.MAX_REPLICA_COUNT; replicaIx++) {
                    assertEquals(partition.getReplica(replicaIx), addresses[replicaIx]);
                }
            }
        }
    }

    private void test(MemberGroupFactory memberGroupFactory) throws Exception {
        PartitionStateGenerator generator = new PartitionStateGeneratorImpl();
        int maxSameHostCount = 3;
        int[] partitionCounts = new int[]{271, 787, 1549, 3217};
        int[] members = new int[]{3, 6, 9, 10, 11, 17, 57, 100, 130, 77, 179, 93, 37, 26, 15, 5};
        for (int partitionCount : partitionCounts) {
            int memberCount = members[0];
            List<Member> memberList = createMembers(memberCount, maxSameHostCount);
            Collection<MemberGroup> groups = memberGroupFactory.createMemberGroups(memberList);
            PartitionReplica[][] state = generator.arrange(groups, emptyPartitionArray(partitionCount));
            checkTestResult(state, groups, partitionCount);
            int previousMemberCount = memberCount;
            for (int j = 1; j < members.length; j++) {
                memberCount = members[j];
                if (partitionCount / memberCount < 10) {
                    break;
                }
                if ((float) partitionCount / memberCount > 2) {
                    if (previousMemberCount == 0) {
                        memberList = createMembers(memberCount, maxSameHostCount);
                    } else if (memberCount > previousMemberCount) {
                        MemberImpl last = (MemberImpl) memberList.get(previousMemberCount - 1);
                        List<Member> extra = createMembers(last, (memberCount - previousMemberCount), maxSameHostCount);
                        memberList.addAll(extra);
                    } else {
                        List<Member> removedMembers = memberList.subList(memberCount, memberList.size());
                        memberList = memberList.subList(0, memberCount);
                        remove(state, removedMembers);
                    }
                    groups = memberGroupFactory.createMemberGroups(memberList);
                    state = generator.arrange(groups, toPartitionArray(state));
                    checkTestResult(state, groups, partitionCount);
                    previousMemberCount = memberCount;
                }
            }
        }
    }

    static InternalPartition[] toPartitionArray(PartitionReplica[][] state) {
        InternalPartition[] result = new InternalPartition[state.length];
        for (int partitionId = 0; partitionId < state.length; partitionId++) {
            result[partitionId] = new ReadonlyInternalPartition(state[partitionId], partitionId, 1);
        }
        return result;
    }

    static InternalPartition[] emptyPartitionArray(int partitionCount) {
        InternalPartition[] result = new InternalPartition[partitionCount];
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            result[partitionId] = new ReadonlyInternalPartition(new PartitionReplica[InternalPartition.MAX_REPLICA_COUNT], partitionId, 0);
        }
        return result;
    }

    private static void remove(PartitionReplica[][] state, List<Member> removedMembers) {
        Set<Address> addresses = new HashSet<Address>();
        for (Member member : removedMembers) {
            addresses.add(member.getAddress());
        }
        for (PartitionReplica[] replicas : state) {
            for (int i = 0; i < replicas.length; i++) {
                if (replicas[i] != null && !addresses.contains(replicas[i].address())) {
                    replicas[i] = null;
                    break;
                }
            }
        }
    }

    static List<Member> createMembers(int memberCount, int maxSameHostCount) throws Exception {
        return createMembers(null, memberCount, maxSameHostCount);
    }

    private static List<Member> createMembers(MemberImpl startAfter, int memberCount, int maxSameHostCount) throws Exception {
        Random rand = new Random();
        final byte[] ip = new byte[]{10, 10, 0, 0};
        if (startAfter != null) {
            Address address = startAfter.getAddress();
            byte[] startIp = address.getInetAddress().getAddress();
            if ((0xff & startIp[3]) < 255) {
                ip[2] = startIp[2];
                ip[3] = (byte) (startIp[3] + 1);
            } else {
                ip[2] = (byte) (startIp[2] + 1);
                ip[3] = 0;
            }
        }
        int count = 0;
        int port = 5700;
        List<Member> members = new ArrayList<Member>();
        int sameHostCount = rand.nextInt(maxSameHostCount) + 1;
        for (int i = 0; i < memberCount; i++) {
            if (count == sameHostCount) {
                ip[3] = ++ip[3];
                count = 0;
                port = 5700;
                sameHostCount = rand.nextInt(maxSameHostCount) + 1;
            }
            count++;
            port++;
            MemberImpl m = new MemberImpl(new Address(InetAddress.getByAddress(new byte[]{ip[0], ip[1], ip[2], ip[3]})
                    , port), VERSION, false, UuidUtil.newUnsecureUUID());
            members.add(m);
            if ((0xff & ip[3]) == 255) {
                ip[2] = ++ip[2];
            }
        }
        return members;
    }

    private void checkTestResult(PartitionReplica[][] state, Collection<MemberGroup> groups, int partitionCount) {
        Iterator<MemberGroup> iter = groups.iterator();
        while (iter.hasNext()) {
            if (iter.next().size() == 0) {
                iter.remove();
            }
        }
        int replicaCount = Math.min(groups.size(), InternalPartition.MAX_REPLICA_COUNT);
        Map<MemberGroup, GroupPartitionState> groupPartitionStates = new HashMap<MemberGroup, GroupPartitionState>();
        Set<PartitionReplica> set = new HashSet<PartitionReplica>();
        int avgPartitionPerGroup = partitionCount / groups.size();

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            PartitionReplica[] replicas = state[partitionId];
            for (int i = 0; i < replicaCount; i++) {
                PartitionReplica owner = replicas[i];
                assertNotNull(owner);
                assertFalse("Duplicate owner of partition: " + partitionId,
                        set.contains(owner));
                set.add(owner);
                MemberGroup group = null;
                for (MemberGroup g : groups) {
                    if (g.hasMember(new MemberImpl(owner.address(), VERSION, true, owner.uuid()))) {
                        group = g;
                        break;
                    }
                }
                assertNotNull(group);
                GroupPartitionState groupState = groupPartitionStates.get(group);
                if (groupState == null) {
                    groupState = new GroupPartitionState();
                    groupState.group = group;
                    groupPartitionStates.put(group, groupState);
                }
                groupState.groupPartitions[i].add(partitionId);
                groupState.getNodePartitions(owner)[i].add(partitionId);
            }
            set.clear();
        }
        for (GroupPartitionState groupState : groupPartitionStates.values()) {
            for (Map.Entry<PartitionReplica, Set<Integer>[]> entry : groupState.nodePartitionsMap.entrySet()) {
                Collection<Integer>[] partitions = entry.getValue();
                for (int i = 0; i < replicaCount; i++) {
                    int avgPartitionPerNode = groupState.groupPartitions[i].size() / groupState.nodePartitionsMap.size();
                    int count = partitions[i].size();
                    isInAllowedRange(count, avgPartitionPerNode, i, entry.getKey(), groups, partitionCount);
                }
            }
            Collection<Integer>[] partitions = groupState.groupPartitions;
            for (int i = 0; i < replicaCount; i++) {
                int count = partitions[i].size();
                isInAllowedRange(count, avgPartitionPerGroup, i, groupState.group, groups, partitionCount);
            }
        }

        printTable(groupPartitionStates, replicaCount);
    }

    private static void isInAllowedRange(int count, int average, int replica,
                                         Object owner, final Collection<MemberGroup> groups, final int partitionCount) {
        if (average <= 10) {
            return;
        }
        final float r = 2f;
        assertTrue("Too low partition count! \nOwned: " + count + ", Avg: " + average
                + ", \nPartitionCount: " + partitionCount + ", Replica: " + replica
                + ", \nOwner: " + owner, count >= (float) (average) / r);

        assertTrue("Too high partition count! \nOwned: " + count + ", Avg: " + average
                + ", \nPartitionCount: " + partitionCount + ", Replica: " + replica
                + ", \nOwner: " + owner, count <= (float) (average) * r);
    }

    private static void printTable(Map<MemberGroup, GroupPartitionState> groupPartitionStates, int replicaCount) {
        if (!PRINT_STATE) {
            return;
        }

        System.out.printf("%-20s", "Owner");
        for (int i = 0; i < replicaCount; i++) {
            System.out.printf("%-5s", "R-" + i);
        }
        System.out.printf("%5s%n", "Total");
        System.out.println("_______________________________________________________________");
        System.out.println();

        int k = 1;
        for (GroupPartitionState groupState : groupPartitionStates.values()) {
            System.out.printf("%-20s%n", "MemberGroup[" + (k++) + "]");

            for (Map.Entry<PartitionReplica, Set<Integer>[]> entry : groupState.nodePartitionsMap.entrySet()) {
                int total = 0;
                Address address = entry.getKey().address();
                System.out.printf("%-20s", address.getHost() + ":" + address.getPort());
                Collection<Integer>[] partitions = entry.getValue();
                for (int i = 0; i < replicaCount; i++) {
                    int count = partitions[i].size();
                    System.out.printf("%-5s", count);
                    total += partitions[i].size();
                }
                System.out.printf("%-5s%n", total);
            }

            if (groupState.group.size() > 1) {
                System.out.printf("%-20s", "Total");
                int total = 0;
                Collection<Integer>[] partitions = groupState.groupPartitions;
                for (int i = 0; i < replicaCount; i++) {
                    int count = partitions[i].size();
                    System.out.printf("%-5s", count);
                    total += partitions[i].size();
                }
                System.out.printf("%-5s%n", total);
            }
            System.out.println("---------------------------------------------------------------");
            System.out.println();
        }
        System.out.println();
        System.out.println();
    }

    private static class GroupPartitionState {
        MemberGroup group;
        Set<Integer>[] groupPartitions = new Set[InternalPartition.MAX_REPLICA_COUNT];
        Map<PartitionReplica, Set<Integer>[]> nodePartitionsMap = new HashMap<PartitionReplica, Set<Integer>[]>();

        {
            for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                groupPartitions[i] = new HashSet<Integer>();
            }
        }

        Set<Integer>[] getNodePartitions(PartitionReplica node) {
            Set<Integer>[] nodePartitions = nodePartitionsMap.get(node);
            if (nodePartitions == null) {
                nodePartitions = new Set[InternalPartition.MAX_REPLICA_COUNT];
                for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                    nodePartitions[i] = new HashSet<Integer>();
                }
                nodePartitionsMap.put(node, nodePartitions);
            }
            return nodePartitions;
        }
    }
}
