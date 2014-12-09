/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition.impl;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.membergroup.ConfigMemberGroupFactory;
import com.hazelcast.partition.membergroup.DefaultMemberGroup;
import com.hazelcast.partition.membergroup.HostAwareMemberGroupFactory;
import com.hazelcast.partition.membergroup.MemberGroup;
import com.hazelcast.partition.membergroup.MemberGroupFactory;
import com.hazelcast.partition.membergroup.SingleMemberGroupFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
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
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class PartitionStateGeneratorTest {

    private static final boolean printState = false;

    @BeforeClass
    @AfterClass
    public static void init() throws IOException {
        Hazelcast.shutdownAll();
    }

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
                    Address address = ((MemberImpl) member).getAddress();
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
        assertFalse(partitionGroupConfig.isEnabled());
        assertEquals(PartitionGroupConfig.MemberGroupType.CUSTOM, partitionGroupConfig.getGroupType());
        assertEquals(2, partitionGroupConfig.getMemberGroupConfigs().size());
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
            println("PARTITION-COUNT= " + partitionCount + ", MEMBER-COUNT= "
                    + members[0] + ", GROUP-COUNT= " + groups.size());
            println();
            Address[][] state = generator.initialize(memberGroupFactory.createMemberGroups(memberList), partitionCount);
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
                        memberList = memberList.subList(0, memberCount);
                        shift(state, memberList);
                    }
                    groups = memberGroupFactory.createMemberGroups(memberList);
                    println("PARTITION-COUNT= " + partitionCount + ", MEMBER-COUNT= " + memberCount + ", GROUP-COUNT= " + groups.size());
                    //todo
                    state = generator.reArrange(memberGroupFactory.createMemberGroups(memberList), toPartitionView(state));
                    checkTestResult(state, groups, partitionCount);
                    previousMemberCount = memberCount;
                }
            }
        }
    }

    private DummyInternalPartition[] toPartitionView(Address[][] state) {
        DummyInternalPartition[] result = new DummyInternalPartition[state.length];
        for (int partitionId = 0; partitionId < state.length; partitionId++) {
            DummyInternalPartition partitionView = new DummyInternalPartition(state[partitionId]);
            result[partitionId] = partitionView;
        }
        return result;
    }

    private class DummyInternalPartition implements InternalPartition {
        private Address[] replicas;

        private DummyInternalPartition(Address[] replicas) {
            this.replicas = replicas;
        }

        @Override
        public int getPartitionId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Address getOwnerOrNull() {
            return replicas[0];
        }

        @Override
        public boolean isMigrating() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Address getReplicaAddress(int replicaIndex) {
            return replicas[replicaIndex];
        }

        @Override
        public boolean isOwnerOrBackup(Address address) {
            throw new UnsupportedOperationException();
        }
    }

    private static void shift(Address[][] state, List<Member> members) {
        Set<Address> addresses = new HashSet<Address>();
        for (Member member : members) {
            addresses.add(((MemberImpl) member).getAddress());
        }
        for (int partitionId = 0; partitionId < state.length; partitionId++) {
            Address[] replicas = state[partitionId];
            for (int i = 0; i < replicas.length; i++) {
                if (replicas[i] != null && !addresses.contains(replicas[i])) {
                    Address[] validAddresses = new Address[InternalPartition.MAX_REPLICA_COUNT - i];
                    int k = 0;
                    for (int a = i + 1; a < InternalPartition.MAX_REPLICA_COUNT; a++) {
                        Address address = replicas[a];
                        if (address != null && addresses.contains(address)) {
                            validAddresses[k++] = address;
                        }
                    }
                    for (int a = 0; a < k; a++) {
                        replicas[i + a] = validAddresses[a];
                    }
                    for (int a = i + k; a < InternalPartition.MAX_REPLICA_COUNT; a++) {
                        replicas[a] = null;
                    }
                    break;
                }
            }
        }
    }

    private static List<Member> createMembers(int memberCount, int maxSameHostCount) throws Exception {
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
                    , port), false);
            members.add(m);
            if ((0xff & ip[3]) == 255) {
                ip[2] = ++ip[2];
            }
        }
        return members;
    }

    private void checkTestResult(final Address[][] state, final Collection<MemberGroup> groups, final int partitionCount) {
        Iterator<MemberGroup> iter = groups.iterator();
        while (iter.hasNext()) {
            if (iter.next().size() == 0) {
                iter.remove();
            }
        }
        final int replicaCount = Math.min(groups.size(), InternalPartition.MAX_REPLICA_COUNT);
        final Map<MemberGroup, GroupPartitionState> groupPartitionStates = new HashMap<MemberGroup, GroupPartitionState>();
        final Set<Address> set = new HashSet<Address>();
        final int avgPartitionPerGroup = partitionCount / groups.size();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            Address[] replicas = state[partitionId];
            for (int i = 0; i < replicaCount; i++) {
                Address owner = replicas[i];
                Assert.assertNotNull(owner);
                assertFalse("Duplicate owner of partition: " + partitionId,
                        set.contains(owner));
                set.add(owner);
                MemberGroup group = null;
                for (MemberGroup g : groups) {
                    if (g.hasMember(new MemberImpl(owner, true))) {
                        group = g;
                        break;
                    }
                }
                Assert.assertNotNull(group);
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
        print("Owner");
        for (int i = 0; i < replicaCount; i++) {
            if (i == 0) {
                print("\t\t");
            }
            print("\tRep-" + i);
        }
        print("\tTotal");
        println();
        println("_______________________________________________________________________________________");
        int k = 1;
        for (GroupPartitionState groupState : groupPartitionStates.values()) {
            for (Map.Entry<Address, Set<Integer>[]> entry : groupState.nodePartitionsMap.entrySet()) {
                int total = 0;
                print(entry.getKey().getHost() + ":" + entry.getKey().getPort());
                Collection<Integer>[] partitions = entry.getValue();
                for (int i = 0; i < replicaCount; i++) {
                    final int avgPartitionPerNode = groupState.groupPartitions[i].size() / groupState.nodePartitionsMap.size();
                    if (i == 0) {
                        print("\t");
                    }
                    print('\t');
                    int count = partitions[i].size();
                    print(count);
                    total += partitions[i].size();
                    isInAllowedRange(count, avgPartitionPerNode, i, entry.getKey(), groups, partitionCount);
                }
                print('\t');
                print(total);
                println();
            }
            println("----------------------------------------------------------------------------------------");
            int total = 0;
            print("Group" + (k++) + "[" + groupState.group.size() + "]");
            Collection<Integer>[] partitions = groupState.groupPartitions;
            for (int i = 0; i < replicaCount; i++) {
                if (i == 0) {
                    print("\t");
                }
                print('\t');
                int count = partitions[i].size();
                print(count);
                total += partitions[i].size();
                isInAllowedRange(count, avgPartitionPerGroup, i, groupState.group, groups, partitionCount);
            }
            print('\t');
            print(total);
            println();
            println();
        }
        println();
        println();
    }

    private static void isInAllowedRange(int count, int average, int replica,
                                         Object owner, final Collection<MemberGroup> groups, final int partitionCount) {
        if (average <= 10) {
            return;
        }
        final float r = 2f;
        assertTrue("Too low partition count! \nOwned: " + count + ", Avg: " + average
                + ", \nPartitionCount: " + partitionCount + ", Replica: " + replica +
                ", \nOwner: " + owner, count >= (float) (average) / r);

        assertTrue("Too high partition count! \nOwned: " + count + ", Avg: " + average
                + ", \nPartitionCount: " + partitionCount + ", Replica: " + replica +
                ", \nOwner: " + owner, count <= (float) (average) * r);
    }

    private static void println(Object str) {
        print(str);
        println();
    }

    private static void println() {
        print('\n');
    }

    private static void print(Object str) {
        if (!printState) {
            return;
        }
        System.out.print(str);
    }

    private static class GroupPartitionState {
        MemberGroup group;
        Set<Integer>[] groupPartitions = new Set[InternalPartition.MAX_REPLICA_COUNT];
        Map<Address, Set<Integer>[]> nodePartitionsMap = new HashMap<Address, Set<Integer>[]>();

        {
            for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                groupPartitions[i] = new HashSet<Integer>();
            }
        }

        Set<Integer>[] getNodePartitions(Address node) {
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
