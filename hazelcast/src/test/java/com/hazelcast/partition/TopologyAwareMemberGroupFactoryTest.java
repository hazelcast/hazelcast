package com.hazelcast.partition;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import org.junit.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * User: grant
 */
public class TopologyAwareMemberGroupFactoryTest {


    @Test
    public void testCreateInternalMemberGroups_Sites() throws Exception {
        TopologyAwareMemberGroupFactory factory = new TopologyAwareMemberGroupFactory();
        List<Member> members = new ArrayList<Member>();
        members.add(new MemberImpl(new Address("site1", "rack1", "localhost", "process1", 5000), true));
        members.add(new MemberImpl(new Address("site2", "rack1", "localhost", "process1", 5000), true));

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
        members.add(new MemberImpl(new Address(null, "rack1", "localhost", "process1", 5000), true));
        members.add(new MemberImpl(new Address(null, "rack2", "localhost", "process1", 5000), true));

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
        members.add(new MemberImpl(new Address(null, null, "localhost", "process1", 5000), true));
        members.add(new MemberImpl(new Address(null, null, InetAddress.getLocalHost().getHostName(), "process1", 5000), true));

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
        members.add(new MemberImpl(new Address(null, null, "localhost", "process1", 5000), true));
        members.add(new MemberImpl(new Address(null, null, "localhost", "process2", 5000), true));

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
        members.add(new MemberImpl(new Address("site1", "rack1", "localhost", "process1", 5000), true));
        members.add(new MemberImpl(new Address("site2", "rack1", "localhost", "process1", 5000), true));
        members.add(new MemberImpl(new Address(null, "rack1", "localhost", "process1", 5000), true));
        members.add(new MemberImpl(new Address(null, "rack2", "localhost", "process1", 5000), true));

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
