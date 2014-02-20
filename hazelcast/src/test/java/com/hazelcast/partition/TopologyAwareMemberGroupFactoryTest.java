package com.hazelcast.partition;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.MemberAttributes;
import org.junit.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * User: grant
 */
public class TopologyAwareMemberGroupFactoryTest {


    @Test
    public void testCreateInternalMemberGroups_Sites() throws Exception {

        TopologyAwareMemberGroupFactory factory = new TopologyAwareMemberGroupFactory();
        List<Member> members = new ArrayList<Member>();
        MemberAttributes attributes1 = new MemberAttributes();
        attributes1.addAttribute(TopologyAwareMemberGroupFactory.HAZELCAST_ADDRESS_SITE, "site1");
        members.add(new MemberImpl(new Address("localhost", 5000), true, attributes1));
        MemberAttributes attributes2 = new MemberAttributes();
        attributes2.addAttribute(TopologyAwareMemberGroupFactory.HAZELCAST_ADDRESS_SITE, "site2");
        members.add(new MemberImpl(new Address("localhost", 5000), true, attributes2));

        Set<MemberGroup> result = factory.createInternalMemberGroups(members);
        assertNotNull(result);
        assertEquals(2, result.size());

        MemberGroup group1 = result.iterator().next();
        assertEquals(1, group1.size());

        MemberGroup group2 = result.iterator().next();
        assertEquals(1, group2.size());


    }

    @Test
    public void testCreateInternalMemberGroups_Racks() throws Exception {
        TopologyAwareMemberGroupFactory factory = new TopologyAwareMemberGroupFactory();
        List<Member> members = new ArrayList<Member>();
        MemberAttributes attributes1 = new MemberAttributes();
        attributes1.addAttribute(TopologyAwareMemberGroupFactory.HAZELCAST_ADDRESS_RACK, "rack1");
        members.add(new MemberImpl(new Address("localhost", 5000), true, attributes1));
        MemberAttributes attributes2 = new MemberAttributes();
        attributes2.addAttribute(TopologyAwareMemberGroupFactory.HAZELCAST_ADDRESS_RACK, "rack2");
        members.add(new MemberImpl(new Address("localhost", 5000), true, attributes2));

        Set<MemberGroup> result = factory.createInternalMemberGroups(members);
        assertNotNull(result);
        assertEquals(2, result.size());

        MemberGroup group1 = result.iterator().next();
        assertEquals(1, group1.size());

        MemberGroup group2 = result.iterator().next();
        assertEquals(1, group2.size());

    }

    @Test
    public void testCreateInternalMemberGroups_Hosts() throws Exception {
        TopologyAwareMemberGroupFactory factory = new TopologyAwareMemberGroupFactory();
        List<Member> members = new ArrayList<Member>();
        members.add(new MemberImpl(new Address("localhost", 5000), true, null, null));
        members.add(new MemberImpl(new Address(InetAddress.getLocalHost().getHostName(),5000), true, null, null));

        Set<MemberGroup> result = factory.createInternalMemberGroups(members);
        assertNotNull(result);
        assertEquals(2, result.size());

        MemberGroup group1 = result.iterator().next();
        assertEquals(1, group1.size());

        MemberGroup group2 = result.iterator().next();
        assertEquals(1, group2.size());


    }

    @Test
    public void testCreateInternalMemberGroups_Processes() throws Exception {
        TopologyAwareMemberGroupFactory factory = new TopologyAwareMemberGroupFactory();
        List<Member> members = new ArrayList<Member>();
        members.add(new MemberImpl(new Address("localhost", 5000), true));
        members.add(new MemberImpl(new Address("localhost", 5000), true));

        Set<MemberGroup> result = factory.createInternalMemberGroups(members);
        assertNotNull(result);
        assertEquals(2, result.size());

        MemberGroup group1 = result.iterator().next();
        assertEquals(1, group1.size());

        MemberGroup group2 = result.iterator().next();
        assertEquals(1, group2.size());


    }



    @Test
    public void testCreateInternalMemberGroups_MixOfSitesAndRacks() throws Exception {
        TopologyAwareMemberGroupFactory factory = new TopologyAwareMemberGroupFactory();
        List<Member> members = new ArrayList<Member>();
        members.add(new MemberImpl(new Address("localhost", 5000), true));
        members.add(new MemberImpl(new Address("localhost", 5000), true));
        members.add(new MemberImpl(new Address("localhost", 5000), true));
        members.add(new MemberImpl(new Address("localhost", 5000), true));

        Set<MemberGroup> result = factory.createInternalMemberGroups(members);
        assertNotNull(result);
        assertEquals(4, result.size());
        MemberGroup group1 = result.iterator().next();
        assertEquals(1, group1.size());
        MemberGroup group2 = result.iterator().next();
        assertEquals(1, group2.size());
        MemberGroup group3 = result.iterator().next();
        assertEquals(1, group3.size());
        MemberGroup group4 = result.iterator().next();
        assertEquals(1, group4.size());

    }

}
