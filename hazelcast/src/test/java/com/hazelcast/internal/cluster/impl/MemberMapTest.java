/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;

import static com.hazelcast.util.UuidUtil.newUnsecureUuidString;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberMapTest {

    @Test
    public void createEmpty() throws Exception {
        MemberMap map = MemberMap.empty();
        assertTrue(map.getMembers().isEmpty());
        assertTrue(map.getAddresses().isEmpty());
        assertEquals(0, map.size());
    }

    @Test
    public void createSingleton() throws Exception {
        MemberImpl member = newMember(5000);
        MemberMap map = MemberMap.singleton(member);
        assertEquals(1, map.getMembers().size());
        assertEquals(1, map.getAddresses().size());
        assertEquals(1, map.size());
        assertTrue(map.contains(member.getAddress()));
        assertTrue(map.contains(member.getUuid()));
        assertThat(member, sameInstance(map.getMember(member.getAddress())));
        assertThat(member, sameInstance(map.getMember(member.getUuid())));

        assertMemberSet(map);
    }

    private void assertMemberSet(MemberMap map) {
        for (MemberImpl m : map.getMembers()) {
            assertTrue(map.contains(m.getUuid()));
            assertTrue(map.contains(m.getAddress()));
            assertEquals(m, map.getMember(m.getAddress()));
            assertEquals(m, map.getMember(m.getUuid()));
        }
    }

    @Test
    public void createNew() throws Exception {
        MemberImpl[] members = new MemberImpl[5];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }

        MemberMap map = MemberMap.createNew(members);
        assertEquals(members.length, map.getMembers().size());
        assertEquals(members.length, map.getAddresses().size());
        assertEquals(members.length, map.size());

        for (MemberImpl member : members) {
            assertTrue(map.contains(member.getAddress()));
            assertTrue(map.contains(member.getUuid()));
            assertThat(member, sameInstance(map.getMember(member.getAddress())));
            assertThat(member, sameInstance(map.getMember(member.getUuid())));
        }

        assertMemberSet(map);
    }

    @Test(expected = IllegalArgumentException.class)
    public void create_failsWithDuplicateAddress() throws Exception {
        MemberImpl member1 = newMember(5000);
        MemberImpl member2 = newMember(5000);
        MemberMap.createNew(member1, member2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void create_failsWithDuplicateUuid() throws Exception {
        MemberImpl member1 = newMember(5000);
        MemberImpl member2 = new MemberImpl(newAddress(5001), false, member1.getUuid(), null);
        MemberMap.createNew(member1, member2);
    }

    @Test
    public void cloneExcluding() throws Exception {
        MemberImpl[] members = new MemberImpl[6];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }

        MemberImpl exclude0 = members[0];
        MemberImpl exclude1 = new MemberImpl(newAddress(6000), false, members[1].getUuid(), null);
        MemberImpl exclude2 = new MemberImpl(members[2].getAddress(), false, newUnsecureUuidString(), null);

        MemberMap map = MemberMap.cloneExcluding(MemberMap.createNew(members), exclude0, exclude1, exclude2);

        int numOfExcludedMembers = 3;
        assertEquals(members.length - numOfExcludedMembers, map.getMembers().size());
        assertEquals(members.length - numOfExcludedMembers, map.getAddresses().size());
        assertEquals(members.length - numOfExcludedMembers, map.size());

        for (int i = 0; i < numOfExcludedMembers; i++) {
            MemberImpl member = members[i];
            assertFalse(map.contains(member.getAddress()));
            assertFalse(map.contains(member.getUuid()));
            assertNull(map.getMember(member.getAddress()));
            assertNull(map.getMember(member.getUuid()));
        }

        for (int i = numOfExcludedMembers; i < members.length; i++) {
            MemberImpl member = members[i];
            assertTrue(map.contains(member.getAddress()));
            assertTrue(map.contains(member.getUuid()));
            assertThat(member, sameInstance(map.getMember(member.getAddress())));
            assertThat(member, sameInstance(map.getMember(member.getUuid())));
        }

        assertMemberSet(map);
    }

    @Test
    public void cloneExcluding_emptyMap() throws Exception {
        MemberMap empty = MemberMap.empty();
        MemberMap map = MemberMap.cloneExcluding(empty, newMember(5000));

        assertEquals(0, map.size());
        assertThat(empty, sameInstance(map));
    }


    @Test
    public void cloneAdding() throws Exception {
        MemberImpl[] members = new MemberImpl[5];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }

        MemberMap map = MemberMap.cloneAdding(MemberMap.createNew(members[0], members[1], members[2]),
                members[3], members[4]);
        assertEquals(members.length, map.getMembers().size());
        assertEquals(members.length, map.getAddresses().size());
        assertEquals(members.length, map.size());

        for (MemberImpl member : members) {
            assertTrue(map.contains(member.getAddress()));
            assertTrue(map.contains(member.getUuid()));
            assertThat(member, sameInstance(map.getMember(member.getAddress())));
            assertThat(member, sameInstance(map.getMember(member.getUuid())));
        }

        assertMemberSet(map);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cloneAdding_failsWithDuplicateAddress() throws Exception {
        MemberImpl[] members = new MemberImpl[3];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }

        MemberImpl member = newMember(5000);
        MemberMap.cloneAdding(MemberMap.createNew(members), member);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cloneAdding_failsWithDuplicateUuid() throws Exception {
        MemberImpl[] members = new MemberImpl[3];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }

        MemberImpl member = new MemberImpl(newAddress(6000), false, members[1].getUuid(), null);
        MemberMap.cloneAdding(MemberMap.createNew(members), member);
    }

    @Test
    public void getMembers_ordered() throws Exception {
        MemberImpl[] members = new MemberImpl[10];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }

        MemberMap map = MemberMap.createNew(members);
        Set<MemberImpl> memberSet = map.getMembers();

        int k = 0;
        for (MemberImpl member : memberSet) {
            assertSame(members[k++], member);
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getMembers_unmodifiable() throws Exception {
        MemberImpl[] members = new MemberImpl[5];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }

        MemberMap map = MemberMap.createNew(members);

        map.getMembers().add(newMember(9000));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getAddresses_unmodifiable() throws Exception {
        MemberImpl[] members = new MemberImpl[5];
        for (int i = 0; i < members.length; i++) {
            members[i] = newMember(5000 + i);
        }

        MemberMap map = MemberMap.createNew(members);

        map.getAddresses().add(newAddress(9000));
    }

    private MemberImpl newMember(int port) throws UnknownHostException {
        return new MemberImpl(newAddress(port), false, newUnsecureUuidString(), null);
    }

    private Address newAddress(int port) throws UnknownHostException {
        return new Address(InetAddress.getLocalHost(), port);
    }

}
