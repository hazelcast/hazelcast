/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition.membergroup;

import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.core.Member;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MemberGroupFactoryTest {

    private static final MemberVersion VERSION = MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion());

    @Test
    public void testHostAwareMemberGroupFactoryCreateMemberGroups() throws Exception {
        MemberGroupFactory groupFactory = new HostAwareMemberGroupFactory();
        Collection<Member> members = createMembers();
        Collection<MemberGroup> memberGroups = groupFactory.createMemberGroups(members);

        assertEquals("Member Groups: " + String.valueOf(memberGroups), 8, memberGroups.size());
        for (MemberGroup memberGroup : memberGroups) {
            assertEquals("Member Group: " + String.valueOf(memberGroup), 2, memberGroup.size());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testZoneAwareMemberGroupFactoryThrowsIllegalArgumentExceptionWhenNoMetadataIsProvided() throws Exception {
        MemberGroupFactory groupFactory = new ZoneAwareMemberGroupFactory();
        Collection<Member> members = createMembersWithNoMetadata();
        Collection<MemberGroup> memberGroups = groupFactory.createMemberGroups(members);

        assertEquals("Member Groups: " + String.valueOf(memberGroups), 3, memberGroups.size());
        for (MemberGroup memberGroup : memberGroups) {
            assertEquals("Member Group: " + String.valueOf(memberGroup), 1, memberGroup.size());
        }
    }

    @Test
    public void testZoneMetadataAwareMemberGroupFactoryCreateMemberGroups() throws Exception {
        MemberGroupFactory groupFactory = new ZoneAwareMemberGroupFactory();
        Collection<Member> members = createMembersWithZoneAwareMetadata();
        Collection<MemberGroup> memberGroups = groupFactory.createMemberGroups(members);

        assertEquals("Member Groups: " + String.valueOf(memberGroups), 3, memberGroups.size());
        for (MemberGroup memberGroup : memberGroups) {
            assertEquals("Member Group: " + String.valueOf(memberGroup), 1, memberGroup.size());
        }
    }

    @Test
    public void testRackMetadataAwareMemberGroupFactoryCreateMemberGroups() throws Exception {
        MemberGroupFactory groupFactory = new ZoneAwareMemberGroupFactory();
        Collection<Member> members = createMembersWithRackAwareMetadata();
        Collection<MemberGroup> memberGroups = groupFactory.createMemberGroups(members);

        assertEquals("Member Groups: " + String.valueOf(memberGroups), 3, memberGroups.size());
        for (MemberGroup memberGroup : memberGroups) {
            assertEquals("Member Group: " + String.valueOf(memberGroup), 1, memberGroup.size());
        }
    }

    @Test
    public void testHostMetadataAwareMemberGroupFactoryCreateMemberGroups() throws Exception {
        MemberGroupFactory groupFactory = new ZoneAwareMemberGroupFactory();
        Collection<Member> members = createMembersWithHostAwareMetadata();
        Collection<MemberGroup> memberGroups = groupFactory.createMemberGroups(members);

        assertEquals("Member Groups: " + String.valueOf(memberGroups), 3, memberGroups.size());
        for (MemberGroup memberGroup : memberGroups) {
            assertEquals("Member Group: " + String.valueOf(memberGroup), 1, memberGroup.size());
        }
    }

    @Test
    public void testConfigMemberGroupFactoryCreateMemberGroups() throws Exception {
        Collection<MemberGroupConfig> groupConfigs = createMemberGroupConfigs();
        MemberGroupFactory groupFactory = new ConfigMemberGroupFactory(groupConfigs);
        Collection<Member> members = createMembers();
        Collection<MemberGroup> memberGroups = groupFactory.createMemberGroups(members);

        assertEquals("Member Groups: " + String.valueOf(memberGroups), 4, memberGroups.size());
        for (MemberGroup memberGroup : memberGroups) {
            assertEquals("Member Group: " + String.valueOf(memberGroup), 2, memberGroup.size());
        }
    }

    private Collection<Member> createMembers() throws UnknownHostException {
        Collection<Member> members = new HashSet<Member>();
        InetAddress fakeAddress = InetAddress.getLocalHost();
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

    private Collection<Member> createMembersWithZoneAwareMetadata() throws UnknownHostException {
        Collection<Member> members = new HashSet<Member>();
        InetAddress fakeAddress = InetAddress.getLocalHost();
        MemberImpl member1 = new MemberImpl(new Address("192.192.0.1", fakeAddress, 5701), VERSION, true);
        member1.setStringAttribute(PartitionGroupMetaData.PARTITION_GROUP_ZONE, "us-east-1");

        MemberImpl member2 = new MemberImpl(new Address("192.192.0.2", fakeAddress, 5701), VERSION, true);
        member2.setStringAttribute(PartitionGroupMetaData.PARTITION_GROUP_ZONE, "us-west-1");

        MemberImpl member3 = new MemberImpl(new Address("192.192.0.3", fakeAddress, 5701), VERSION, true);
        member3.setStringAttribute(PartitionGroupMetaData.PARTITION_GROUP_ZONE, "eu-central-1");

        members.add(member1);
        members.add(member2);
        members.add(member3);
        return members;
    }

    private Collection<Member> createMembersWithRackAwareMetadata() throws UnknownHostException {
        Collection<Member> members = new HashSet<Member>();
        InetAddress fakeAddress = InetAddress.getLocalHost();
        MemberImpl member1 = new MemberImpl(new Address("192.192.0.1", fakeAddress, 5701), VERSION, true);
        member1.setStringAttribute(PartitionGroupMetaData.PARTITION_GROUP_RACK, "rack-1");

        MemberImpl member2 = new MemberImpl(new Address("192.192.0.2", fakeAddress, 5701), VERSION, true);
        member2.setStringAttribute(PartitionGroupMetaData.PARTITION_GROUP_RACK, "rack-2");

        MemberImpl member3 = new MemberImpl(new Address("192.192.0.3", fakeAddress, 5701), VERSION, true);
        member3.setStringAttribute(PartitionGroupMetaData.PARTITION_GROUP_RACK, "rack-3");

        members.add(member1);
        members.add(member2);
        members.add(member3);
        return members;
    }

    private Collection<Member> createMembersWithHostAwareMetadata() throws UnknownHostException {
        Collection<Member> members = new HashSet<Member>();
        InetAddress fakeAddress = InetAddress.getLocalHost();
        MemberImpl member1 = new MemberImpl(new Address("192.192.0.1", fakeAddress, 5701), VERSION, true);
        member1.setStringAttribute(PartitionGroupMetaData.PARTITION_GROUP_HOST, "host-1");

        MemberImpl member2 = new MemberImpl(new Address("192.192.0.2", fakeAddress, 5701), VERSION, true);
        member2.setStringAttribute(PartitionGroupMetaData.PARTITION_GROUP_HOST, "host-2");

        MemberImpl member3 = new MemberImpl(new Address("192.192.0.3", fakeAddress, 5701), VERSION, true);
        member3.setStringAttribute(PartitionGroupMetaData.PARTITION_GROUP_HOST, "host-3");

        members.add(member1);
        members.add(member2);
        members.add(member3);
        return members;
    }

    private Collection<Member> createMembersWithNoMetadata() throws UnknownHostException {
        Collection<Member> members = new HashSet<Member>();
        InetAddress fakeAddress = InetAddress.getLocalHost();
        members.add(new MemberImpl(new Address("192.192.0.1", fakeAddress, 5701), VERSION, false));
        members.add(new MemberImpl(new Address("192.192.0.1", fakeAddress, 5702), VERSION, false));
        members.add(new MemberImpl(new Address("192.168.3.101", fakeAddress, 5701), VERSION, false));
        return members;
    }

    private Collection<MemberGroupConfig> createMemberGroupConfigs() {
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
        groupConfigs.add(group3);
        groupConfigs.add(group4);
        return groupConfigs;
    }
}
