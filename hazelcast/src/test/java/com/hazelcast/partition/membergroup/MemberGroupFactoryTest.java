package com.hazelcast.partition.membergroup;

import com.hazelcast.config.MemberGroupConfig;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
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

    @Test
    @Category(ProblematicTest.class)
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
        members.add(new MemberImpl(new Address("192.192.0.1", fakeAddress, 5701), false));
        members.add(new MemberImpl(new Address("192.192.0.1", fakeAddress, 5702), false));
        members.add(new MemberImpl(new Address("192.168.3.101", fakeAddress, 5701), false));
        members.add(new MemberImpl(new Address("192.168.3.101", fakeAddress, 5702), false));
        members.add(new MemberImpl(new Address("172.16.5.11", fakeAddress, 5701), false));
        members.add(new MemberImpl(new Address("172.16.5.11", fakeAddress, 5702), false));
        members.add(new MemberImpl(new Address("172.123.0.13", fakeAddress, 5701), false));
        members.add(new MemberImpl(new Address("172.123.0.13", fakeAddress, 5702), false));
        members.add(new MemberImpl(new Address("www.hazelcast.com.tr", fakeAddress, 5701), false));
        members.add(new MemberImpl(new Address("www.hazelcast.com.tr", fakeAddress, 5702), false));
        members.add(new MemberImpl(new Address("jobs.hazelcast.com", fakeAddress, 5701), false));
        members.add(new MemberImpl(new Address("jobs.hazelcast.com", fakeAddress, 5702), false));
        members.add(new MemberImpl(new Address("www.hazelcast.org", fakeAddress, 5701), false));
        members.add(new MemberImpl(new Address("www.hazelcast.org", fakeAddress, 5702), false));
        members.add(new MemberImpl(new Address("download.hazelcast.org", fakeAddress, 5701), false));
        members.add(new MemberImpl(new Address("download.hazelcast.org", fakeAddress, 5702), false));
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
