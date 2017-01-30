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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.util.UuidUtil.newUnsecureUuidString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberMapTest {

    private static final MemberVersion VERSION = MemberVersion.of(BuildInfoProvider.BUILD_INFO.getVersion());

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testConstructor_whenMapsHaveDifferentMembers_thenThrowAssertionError() {
        Map<Address, MemberImpl> addressMap = new HashMap<Address, MemberImpl>();
        Map<String, MemberImpl> uuidMap = new HashMap<String, MemberImpl>();

        MemberImpl addressMember = newMember(5701);
        MemberImpl uuidMember = newMember(5702);

        addressMap.put(addressMember.getAddress(), addressMember);
        uuidMap.put(uuidMember.getUuid(), uuidMember);

        new MemberMap(addressMap, uuidMap);
    }

    @Test
    public void createEmpty() {
        MemberMap map = MemberMap.empty();
        assertTrue(map.getMembers().isEmpty());
        assertTrue(map.getAddresses().isEmpty());
        assertEquals(0, map.size());
    }

    @Test
    public void createSingleton() {
        MemberImpl member = newMember(5000);
        MemberMap map = MemberMap.singleton(member);
        assertEquals(1, map.getMembers().size());
        assertEquals(1, map.getAddresses().size());
        assertEquals(1, map.size());
        assertContains(map, member.getAddress());
        assertContains(map, member.getUuid());
        assertSame(member, map.getMember(member.getAddress()));
        assertSame(member, map.getMember(member.getUuid()));

        assertMemberSet(map);
    }

    @Test
    public void createNew() {
        MemberImpl[] members = new MemberImpl[5];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }

        MemberMap map = MemberMap.createNew(members);
        assertEquals(members.length, map.getMembers().size());
        assertEquals(members.length, map.getAddresses().size());
        assertEquals(members.length, map.size());

        for (MemberImpl member : members) {
            assertContains(map, member.getAddress());
            assertContains(map, member.getUuid());
            assertSame(member, map.getMember(member.getAddress()));
            assertSame(member, map.getMember(member.getUuid()));
        }

        assertMemberSet(map);
    }

    @Test(expected = IllegalArgumentException.class)
    public void create_failsWithDuplicateAddress() {
        MemberImpl member1 = newMember(5000);
        MemberImpl member2 = newMember(5000);
        MemberMap.createNew(member1, member2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void create_failsWithDuplicateUuid() {
        MemberImpl member1 = newMember(5000);
        MemberImpl member2 = new MemberImpl(newAddress(5001), VERSION, false, member1.getUuid(), null);
        MemberMap.createNew(member1, member2);
    }

    @Test
    public void cloneExcluding() {
        MemberImpl[] members = new MemberImpl[6];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }

        MemberImpl exclude0 = members[0];
        MemberImpl exclude1 = new MemberImpl(newAddress(6000), VERSION, false, members[1].getUuid(), null);
        MemberImpl exclude2 = new MemberImpl(members[2].getAddress(), VERSION, false, newUnsecureUuidString(), null);

        MemberMap map = MemberMap.cloneExcluding(MemberMap.createNew(members), exclude0, exclude1, exclude2);

        int numOfExcludedMembers = 3;
        assertEquals(members.length - numOfExcludedMembers, map.getMembers().size());
        assertEquals(members.length - numOfExcludedMembers, map.getAddresses().size());
        assertEquals(members.length - numOfExcludedMembers, map.size());

        for (int i = 0; i < numOfExcludedMembers; i++) {
            MemberImpl member = members[i];
            assertNotContains(map, member.getAddress());
            assertNotContains(map, member.getUuid());
            assertNull(map.getMember(member.getAddress()));
            assertNull(map.getMember(member.getUuid()));
        }

        for (int i = numOfExcludedMembers; i < members.length; i++) {
            MemberImpl member = members[i];
            assertContains(map, member.getAddress());
            assertContains(map, member.getUuid());
            assertSame(member, map.getMember(member.getAddress()));
            assertSame(member, map.getMember(member.getUuid()));
        }

        assertMemberSet(map);
    }

    @Test
    public void cloneExcluding_emptyMap() {
        MemberMap empty = MemberMap.empty();
        MemberMap map = MemberMap.cloneExcluding(empty, newMember(5000));

        assertEquals(0, map.size());
        assertSame(empty, map);
    }

    @Test
    public void cloneAdding() {
        MemberImpl[] members = new MemberImpl[5];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }

        MemberMap map = MemberMap.cloneAdding(MemberMap.createNew(members[0], members[1], members[2]), members[3], members[4]);
        assertEquals(members.length, map.getMembers().size());
        assertEquals(members.length, map.getAddresses().size());
        assertEquals(members.length, map.size());

        for (MemberImpl member : members) {
            assertContains(map, member.getAddress());
            assertContains(map, member.getUuid());
            assertSame(member, map.getMember(member.getAddress()));
            assertSame(member, map.getMember(member.getUuid()));
        }

        assertMemberSet(map);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cloneAdding_failsWithDuplicateAddress() {
        MemberImpl[] members = new MemberImpl[3];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }

        MemberImpl member = newMember(5000);
        MemberMap.cloneAdding(MemberMap.createNew(members), member);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cloneAdding_failsWithDuplicateUuid() {
        MemberImpl[] members = new MemberImpl[3];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }

        MemberImpl member = new MemberImpl(newAddress(6000), VERSION, false, members[1].getUuid(), null);
        MemberMap.cloneAdding(MemberMap.createNew(members), member);
    }

    @Test
    public void getMembers_ordered() {
        MemberImpl[] members = new MemberImpl[10];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }

        MemberMap map = MemberMap.createNew(members);
        Set<MemberImpl> memberSet = map.getMembers();

        int index = 0;
        for (MemberImpl member : memberSet) {
            assertSame(members[index++], member);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getMembers_unmodifiable() {
        MemberImpl[] members = new MemberImpl[5];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }

        MemberMap map = MemberMap.createNew(members);

        map.getMembers().add(newMember(9000));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getAddresses_unmodifiable() {
        MemberImpl[] members = new MemberImpl[5];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }

        MemberMap map = MemberMap.createNew(members);

        map.getAddresses().add(newAddress(9000));
    }

    private static MemberImpl newMember(int port) {
        return new MemberImpl(newAddress(port), VERSION, false, newUnsecureUuidString(), null);
    }

    private static Address newAddress(int port) {
        try {
            return new Address(InetAddress.getLocalHost(), port);
        } catch (UnknownHostException e) {
            fail("Could not create new Address: " + e.getMessage());
        }
        return null;
    }

    private static void assertMemberSet(MemberMap map) {
        for (MemberImpl member : map.getMembers()) {
            assertContains(map, member.getAddress());
            assertContains(map, member.getUuid());
            assertEquals(member, map.getMember(member.getAddress()));
            assertEquals(member, map.getMember(member.getUuid()));
        }
    }

    private static void assertContains(MemberMap map, Address address) {
        assertTrue("MemberMap doesn't contain expected " + address, map.contains(address));
    }

    private static void assertContains(MemberMap map, String uuid) {
        assertTrue("MemberMap doesn't contain expected " + uuid, map.contains(uuid));
    }

    private static void assertNotContains(MemberMap map, Address address) {
        assertFalse("MemberMap contains unexpected " + address, map.contains(address));
    }

    private static void assertNotContains(MemberMap map, String uuid) {
        assertFalse("MemberMap contains unexpected " + uuid, map.contains(uuid));
    }
}
