package com.hazelcast.partition;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import org.junit.Test;

import java.net.InetAddress;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * User: grant
 */
public class TopologyAwareMemberGroupFactoryTest {


    @Test
    public void testCreateInternalMemberGroupsSites() throws Exception {

        TopologyAwareMemberGroupFactory factory = new TopologyAwareMemberGroupFactory();
        List<Member> members = new ArrayList<Member>();
        Map<String, Object> attributes1 = new HashMap<String, Object>();
        attributes1.put(TopologyAwareMemberGroupFactory.HAZELCAST_ADDRESS_SITE, "site1");
        members.add(new MemberImpl(new Address("localhost", 5000), true, null, null, attributes1));
        Map<String, Object> attributes2 = new HashMap<String, Object>();
        attributes2.put(TopologyAwareMemberGroupFactory.HAZELCAST_ADDRESS_SITE, "site2");
        members.add(new MemberImpl(new Address("localhost", 5001), true, null, null, attributes2));

        Set<MemberGroup> result = factory.createInternalMemberGroups(members);
        assertNotNull(result);
        assertEquals(2, result.size());

        MemberGroup group1 = result.iterator().next();
        assertEquals(1, group1.size());

        MemberGroup group2 = result.iterator().next();
        assertEquals(1, group2.size());


    }

    @Test
    public void testCreateInternalMemberGroupsRacks() throws Exception {
        TopologyAwareMemberGroupFactory factory = new TopologyAwareMemberGroupFactory();
        List<Member> members = new ArrayList<Member>();
        Map<String, Object> attributes1 = new HashMap<String, Object>();
        attributes1.put(TopologyAwareMemberGroupFactory.HAZELCAST_ADDRESS_RACK, "rack1");
        members.add(new MemberImpl(new Address("localhost", 5000), true, null, null, attributes1));
        Map<String, Object> attributes2 = new HashMap<String, Object>();
        attributes2.put(TopologyAwareMemberGroupFactory.HAZELCAST_ADDRESS_RACK, "rack2");
        members.add(new MemberImpl(new Address("localhost", 5001), true, null, null, attributes2));

        Set<MemberGroup> result = factory.createInternalMemberGroups(members);
        assertNotNull(result);
        assertEquals(2, result.size());

        MemberGroup group1 = result.iterator().next();
        assertEquals(1, group1.size());

        MemberGroup group2 = result.iterator().next();
        assertEquals(1, group2.size());

    }

    @Test
    public void testCreateInternalMemberGroupsHosts() throws Exception {
        TopologyAwareMemberGroupFactory factory = new TopologyAwareMemberGroupFactory();
        List<Member> members = new ArrayList<Member>();
        members.add(new MemberImpl(new Address("localhost", 5000), true, null, null));
        members.add(new MemberImpl(new Address(InetAddress.getLocalHost().getHostName(),5001), true, null, null));

        Set<MemberGroup> result = factory.createInternalMemberGroups(members);
        assertNotNull(result);
        assertEquals(2, result.size());

        MemberGroup group1 = result.iterator().next();
        assertEquals(1, group1.size());

        MemberGroup group2 = result.iterator().next();
        assertEquals(1, group2.size());


    }

    @Test
    public void testCreateInternalMemberGroupsProcesses() throws Exception {
        TopologyAwareMemberGroupFactory factory = new TopologyAwareMemberGroupFactory();
        List<Member> members = new ArrayList<Member>();

        Map<String, Object> attributes1 = new HashMap<String, Object>();
        attributes1.put(TopologyAwareMemberGroupFactory.HAZELCAST_ADDRESS_HOST, "host1");
        members.add(new MemberImpl(new Address("localhost", 5000), true, null, null, attributes1));

        Map<String, Object> attributes2 = new HashMap<String, Object>();
        attributes2.put(TopologyAwareMemberGroupFactory.HAZELCAST_ADDRESS_HOST, "host2");
        members.add(new MemberImpl(new Address("localhost", 5001), true, null, null, attributes2));

        Set<MemberGroup> result = factory.createInternalMemberGroups(members);
        assertNotNull(result);
        assertEquals(2, result.size());

        MemberGroup group1 = result.iterator().next();
        assertEquals(1, group1.size());

        MemberGroup group2 = result.iterator().next();
        assertEquals(1, group2.size());


    }



    @Test
    public void testCreateInternalMemberGroupsMixOfSitesAndRacks() throws Exception {
        TopologyAwareMemberGroupFactory factory = new TopologyAwareMemberGroupFactory();
        List<Member> members = new ArrayList<Member>();

        Map<String, Object> attributes1 = new HashMap<String, Object>();
        attributes1.put(TopologyAwareMemberGroupFactory.HAZELCAST_ADDRESS_HOST, "host1");
        members.add(new MemberImpl(new Address("localhost", 5000), true, null, null, attributes1));

        Map<String, Object> attributes2 = new HashMap<String, Object>();
        attributes2.put(TopologyAwareMemberGroupFactory.HAZELCAST_ADDRESS_HOST, "host2");
        members.add(new MemberImpl(new Address("localhost", 5001), true, null, null, attributes2));

        Map<String, Object> attributes3 = new HashMap<String, Object>();
        attributes3.put(TopologyAwareMemberGroupFactory.HAZELCAST_ADDRESS_RACK, "rack1");
        members.add(new MemberImpl(new Address("localhost", 5002), true, null, null, attributes3));

        Map<String, Object> attributes4 = new HashMap<String, Object>();
        attributes4.put(TopologyAwareMemberGroupFactory.HAZELCAST_ADDRESS_RACK, "rack2");
        members.add(new MemberImpl(new Address("localhost", 5003), true, null, null, attributes4));

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
