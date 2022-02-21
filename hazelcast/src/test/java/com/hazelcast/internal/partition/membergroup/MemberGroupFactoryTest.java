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

package com.hazelcast.internal.partition.membergroup;

import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.cluster.Member;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.partitiongroup.MemberGroup;
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MemberGroupFactoryTest {

    private static final MemberVersion VERSION = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());

    private InetAddress fakeAddress;

    @Before
    public void setUp() throws Exception {
        fakeAddress = InetAddress.getLocalHost();
    }

    @Test
    public void testHostAwareMemberGroupFactoryCreateMemberGroups() {
        MemberGroupFactory groupFactory = new HostAwareMemberGroupFactory();
        Collection<Member> members = createMembers();
        Collection<MemberGroup> memberGroups = groupFactory.createMemberGroups(members);

        assertEquals("Member Groups: " + String.valueOf(memberGroups), 8, memberGroups.size());
        for (MemberGroup memberGroup : memberGroups) {
            assertEquals("Member Group: " + String.valueOf(memberGroup), 2, memberGroup.size());
        }
    }

    @Test
    public void testZoneAwareMemberGroupFactoryThrowsIllegalArgumentExceptionWhenNoMetadataIsProvided() {
        MemberGroupFactory groupFactory = new ZoneAwareMemberGroupFactory();
        Collection<Member> members = createMembersWithNoMetadata();
        assertThrows(IllegalArgumentException.class, () -> groupFactory.createMemberGroups(members));
    }

    private Collection<Member> createMembersWithNoMetadata() {
        Collection<Member> members = new HashSet<Member>();
        members.add(new MemberImpl(new Address("192.192.0.1", fakeAddress, 5701), VERSION, false));
        members.add(new MemberImpl(new Address("192.192.0.1", fakeAddress, 5702), VERSION, false));
        members.add(new MemberImpl(new Address("192.168.3.101", fakeAddress, 5701), VERSION, false));
        return members;
    }

    @Test
    public void testZoneMetadataAwareMemberGroupFactoryCreateMemberGroups() {
        MemberGroupFactory groupFactory = new ZoneAwareMemberGroupFactory();
        Collection<Member> members = createMembersWithZoneAwareMetadata();
        Collection<MemberGroup> memberGroups = groupFactory.createMemberGroups(members);

        assertEquals("Member Groups: " + String.valueOf(memberGroups), 3, memberGroups.size());
        for (MemberGroup memberGroup : memberGroups) {
            assertEquals("Member Group: " + String.valueOf(memberGroup), 1, memberGroup.size());
        }
    }

    private Collection<Member> createMembersWithZoneAwareMetadata() {
        Collection<Member> members = new HashSet<Member>();
        MemberImpl member1 = new MemberImpl(new Address("192.192.0.1", fakeAddress, 5701), VERSION, true);
        member1.setAttribute(PartitionGroupMetaData.PARTITION_GROUP_ZONE, "us-east-1");

        MemberImpl member2 = new MemberImpl(new Address("192.192.0.2", fakeAddress, 5701), VERSION, true);
        member2.setAttribute(PartitionGroupMetaData.PARTITION_GROUP_ZONE, "us-west-1");

        MemberImpl member3 = new MemberImpl(new Address("192.192.0.3", fakeAddress, 5701), VERSION, true);
        member3.setAttribute(PartitionGroupMetaData.PARTITION_GROUP_ZONE, "eu-central-1");

        members.add(member1);
        members.add(member2);
        members.add(member3);
        return members;
    }

    @Test
    public void testNodeMetadataAwareMemberGroupFactoryCreateMemberGroups() {
        MemberGroupFactory groupFactory = new NodeAwareMemberGroupFactory();
        Collection<Member> members = createMembersWithNodeAwareMetadata();
        Collection<MemberGroup> memberGroups = groupFactory.createMemberGroups(members);

        assertEquals("Member Groups: " + String.valueOf(memberGroups), 3, memberGroups.size());
        for (MemberGroup memberGroup : memberGroups) {
            assertEquals("Member Group: " + String.valueOf(memberGroup), 1, memberGroup.size());
        }
    }

    private Collection<Member> createMembersWithNodeAwareMetadata() {
        Collection<Member> members = new HashSet<Member>();
        MemberImpl member1 = new MemberImpl(new Address("192.192.0.1", fakeAddress, 5701), VERSION, true);
        member1.setAttribute(PartitionGroupMetaData.PARTITION_GROUP_NODE, "kubernetes-node-f0bbd602-f7cw");

        MemberImpl member2 = new MemberImpl(new Address("192.192.0.2", fakeAddress, 5701), VERSION, true);
        member2.setAttribute(PartitionGroupMetaData.PARTITION_GROUP_NODE, "kubernetes-node-f0bbd602-hgdl");

        MemberImpl member3 = new MemberImpl(new Address("192.192.0.3", fakeAddress, 5701), VERSION, true);
        member3.setAttribute(PartitionGroupMetaData.PARTITION_GROUP_NODE, "kubernetes-node-f0bbd602-0zjs");

        members.add(member1);
        members.add(member2);
        members.add(member3);
        return members;
    }

    @Test
    public void testNodeAwareMemberGroupFactoryThrowsIllegalArgumentExceptionWhenNoMetadataIsProvided() {
        MemberGroupFactory groupFactory = new NodeAwareMemberGroupFactory();
        Collection<Member> members = createMembersWithNoMetadata();
        assertThrows(IllegalArgumentException.class, () -> groupFactory.createMemberGroups(members));
    }

    @Test
    public void testPlacementMetadataAwareMemberGroupFactoryCreateMemberGroups() {
        MemberGroupFactory groupFactory = new PlacementAwareMemberGroupFactory();
        Collection<Member> members = createMembersWithPlacementAwareMetadata();
        Collection<MemberGroup> memberGroups = groupFactory.createMemberGroups(members);

        assertEquals("Member Groups: " + memberGroups, 3, memberGroups.size());
        for (MemberGroup memberGroup : memberGroups) {
            assertEquals("Member Group: " + memberGroup, 1, memberGroup.size());
        }
    }

    private Collection<Member> createMembersWithPlacementAwareMetadata() {
        Collection<Member> members = new HashSet<>();
        MemberImpl member1 = new MemberImpl(new Address("192.192.0.1", fakeAddress, 5701), VERSION, true);
        member1.setAttribute(PartitionGroupMetaData.PARTITION_GROUP_PLACEMENT, "us-east-1a-placement-1");

        MemberImpl member2 = new MemberImpl(new Address("192.192.0.2", fakeAddress, 5701), VERSION, true);
        member2.setAttribute(PartitionGroupMetaData.PARTITION_GROUP_PLACEMENT, "us-east-1a-placement-2");

        MemberImpl member3 = new MemberImpl(new Address("192.192.0.3", fakeAddress, 5701), VERSION, true);
        member3.setAttribute(PartitionGroupMetaData.PARTITION_GROUP_PLACEMENT, "us-east-1a-placement-3");

        members.add(member1);
        members.add(member2);
        members.add(member3);
        return members;
    }

    @Test
    public void testPlacementAwareMemberGroupFactoryThrowsIllegalArgumentExceptionWhenNoMetadataIsProvided() {
        MemberGroupFactory groupFactory = new PlacementAwareMemberGroupFactory();
        Collection<Member> members = createMembersWithNoMetadata();
        assertThrows(IllegalArgumentException.class, () -> groupFactory.createMemberGroups(members));
    }

    /**
     * When there is a matching {@link MemberGroupConfig} for a {@link Member}, it will be assigned to a {@link MemberGroup}.
     * <p>
     * In this test all members will have a matching configuration, so there will be 4 groups with 2 members each.
     */
    @Test
    public void testConfigMemberGroupFactoryCreateMemberGroups() {
        Collection<Member> members = createMembers();
        Collection<MemberGroupConfig> groupConfigs = createMemberGroupConfigs(true);
        MemberGroupFactory groupFactory = new ConfigMemberGroupFactory(groupConfigs);
        Collection<MemberGroup> memberGroups = groupFactory.createMemberGroups(members);

        assertEquals("Member Groups: " + String.valueOf(memberGroups), 4, memberGroups.size());
        for (MemberGroup memberGroup : memberGroups) {
            assertEquals("Member Group: " + String.valueOf(memberGroup), 2, memberGroup.size());
        }
    }

    /**
     * When there is a matching {@link MemberGroupConfig} for a {@link Member}, it will be assigned to a {@link MemberGroup}.
     * <p>
     * In this test half of the members will have a matching configuration, so there will be 2 groups with 2 members each.
     */
    @Test
    public void testConfigMemberGroupFactoryCreateMemberGroups_withNonMatchingMembers() {
        Collection<Member> members = createMembers();
        Collection<MemberGroupConfig> groupConfigs = createMemberGroupConfigs(false);
        MemberGroupFactory groupFactory = new ConfigMemberGroupFactory(groupConfigs);
        Collection<MemberGroup> memberGroups = groupFactory.createMemberGroups(members);

        assertEquals("Member Groups: " + String.valueOf(memberGroups), 2, memberGroups.size());
        for (MemberGroup memberGroup : memberGroups) {
            assertEquals("Member Group: " + String.valueOf(memberGroup), 2, memberGroup.size());
        }
    }

    private Collection<Member> createMembers() {
        Collection<Member> members = new HashSet<Member>();
        members.add(new MemberImpl(new Address("192.192.0.1", fakeAddress, 5701), VERSION, false));
        members.add(new MemberImpl(new Address("192.192.0.1", fakeAddress, 5702), VERSION, false));
        members.add(new MemberImpl(new Address("192.168.3.101", fakeAddress, 5701), VERSION, false));
        members.add(new MemberImpl(new Address("192.168.3.101", fakeAddress, 5702), VERSION, false));

        members.add(new MemberImpl(new Address("172.16.5.11", fakeAddress, 5701), VERSION, false));
        members.add(new MemberImpl(new Address("172.16.5.11", fakeAddress, 5702), VERSION, false));
        members.add(new MemberImpl(new Address("172.123.0.13", fakeAddress, 5701), VERSION, false));
        members.add(new MemberImpl(new Address("172.123.0.13", fakeAddress, 5702), VERSION, false));

        members.add(new MemberImpl(new Address("www.hazelcast.com.tr", fakeAddress, 5701), VERSION, false));
        members.add(new MemberImpl(new Address("www.hazelcast.com.tr", fakeAddress, 5702), VERSION, false));
        members.add(new MemberImpl(new Address("jobs.hazelcast.com", fakeAddress, 5701), VERSION, false));
        members.add(new MemberImpl(new Address("jobs.hazelcast.com", fakeAddress, 5702), VERSION, false));

        members.add(new MemberImpl(new Address("www.hazelcast.org", fakeAddress, 5701), VERSION, false));
        members.add(new MemberImpl(new Address("www.hazelcast.org", fakeAddress, 5702), VERSION, false));
        members.add(new MemberImpl(new Address("download.hazelcast.org", fakeAddress, 5701), VERSION, false));
        members.add(new MemberImpl(new Address("download.hazelcast.org", fakeAddress, 5702), VERSION, false));
        return members;
    }

    private Collection<MemberGroupConfig> createMemberGroupConfigs(boolean addHostnameConfigs) {
        Collection<MemberGroupConfig> groupConfigs = new HashSet<MemberGroupConfig>();

        MemberGroupConfig group1 = new MemberGroupConfig();
        group1.addInterface("192.168.*.*");

        MemberGroupConfig group2 = new MemberGroupConfig();
        group2.addInterface("172.16.*.*");

        MemberGroupConfig group3 = new MemberGroupConfig();
        group3.addInterface("*.hazelcast.com");

        MemberGroupConfig group4 = new MemberGroupConfig();
        group4.addInterface("www.hazelcast.org");

        groupConfigs.add(group1);
        groupConfigs.add(group2);
        if (addHostnameConfigs) {
            groupConfigs.add(group3);
            groupConfigs.add(group4);
        }
        return groupConfigs;
    }
}
