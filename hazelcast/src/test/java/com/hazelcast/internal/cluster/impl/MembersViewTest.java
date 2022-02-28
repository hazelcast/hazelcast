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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.hazelcast.internal.cluster.impl.MemberMapTest.newMember;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MembersViewTest {

    @Test
    public void createNew() {
        int version = 7;
        MemberImpl[] members = MemberMapTest.newMembers(5);
        MembersView view = MembersView.createNew(version, Arrays.asList(members));

        assertEquals(version, view.getVersion());
        assertMembersViewEquals(members, view);
    }

    @Test
    public void cloneAdding() {
        int version = 6;
        MemberImpl[] members = MemberMapTest.newMembers(4);
        List<MemberInfo> additionalMembers
                = Arrays.asList(new MemberInfo(newMember(6000)), new MemberInfo(newMember(7000)));

        MembersView view =
                MembersView.cloneAdding(MembersView.createNew(version, Arrays.asList(members)), additionalMembers);

        assertEquals(version + additionalMembers.size(), view.getVersion());

        MemberImpl[] newMembers = Arrays.copyOf(members, members.length + additionalMembers.size());
        for (int i = 0; i < additionalMembers.size(); i++) {
            newMembers[members.length + i] = additionalMembers.get(i).toMember();
        }

        assertMembersViewEquals(newMembers, view);
    }

    @Test
    public void toMemberMap() {
        int version = 5;
        MemberImpl[] members = MemberMapTest.newMembers(3);
        MembersView view = MembersView.createNew(version, Arrays.asList(members));

        MemberMap memberMap = view.toMemberMap();

        assertEquals(version, memberMap.getVersion());
        assertMembersViewEquals(memberMap.getMembers().toArray(new MemberImpl[0]), view);
    }

    @Test
    public void containsAddress() {
        MemberImpl[] members = MemberMapTest.newMembers(3);
        MembersView view = MembersView.createNew(1, Arrays.asList(members));

        for (MemberImpl member : members) {
            assertTrue(view.containsAddress(member.getAddress()));
        }
    }

    @Test
    public void containsMember() {
        MemberImpl[] members = MemberMapTest.newMembers(3);
        MembersView view = MembersView.createNew(1, Arrays.asList(members));

        for (MemberImpl member : members) {
            assertTrue(view.containsMember(member.getAddress(), member.getUuid()));
        }
    }

    @Test
    public void getAddresses() {
        MemberImpl[] members = MemberMapTest.newMembers(3);
        MembersView view = MembersView.createNew(1, Arrays.asList(members));

        Set<Address> addresses = view.getAddresses();
        assertEquals(members.length, addresses.size());

        for (MemberImpl member : members) {
            assertTrue(addresses.contains(member.getAddress()));
        }
    }

    @Test
    public void getMember() {
        MemberImpl[] members = MemberMapTest.newMembers(3);
        MembersView view = MembersView.createNew(1, Arrays.asList(members));

        MemberInfo member = view.getMember(members[0].getAddress());
        assertNotNull(member);
        assertEquals(members[0].getUuid(), member.getUuid());
    }

    @Test
    public void isLaterThan() {
        MembersView view1 = MembersView.createNew(1, Arrays.asList(MemberMapTest.newMembers(5)));
        MembersView view2 = MembersView.createNew(3, Arrays.asList(MemberMapTest.newMembers(5)));
        MembersView view3 = MembersView.createNew(5, Arrays.asList(MemberMapTest.newMembers(5)));

        assertTrue(view2.isLaterThan(view1));
        assertTrue(view3.isLaterThan(view1));
        assertTrue(view3.isLaterThan(view2));

        assertFalse(view1.isLaterThan(view1));
        assertFalse(view1.isLaterThan(view2));
        assertFalse(view1.isLaterThan(view3));

        assertFalse(view2.isLaterThan(view2));
        assertFalse(view2.isLaterThan(view3));

        assertFalse(view3.isLaterThan(view3));
    }

    private static void assertMembersViewEquals(MemberImpl[] members, MembersView view) {
        assertEquals(members.length, view.size());
        List<MemberInfo> membersInfos = view.getMembers();
        for (int i = 0; i < members.length; i++) {
            assertEquals(members[i], membersInfos.get(i).toMember());
        }
    }
}
