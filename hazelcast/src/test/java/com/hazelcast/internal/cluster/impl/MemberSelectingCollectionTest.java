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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.LITE_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.and;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberSelectingCollectionTest extends HazelcastTestSupport {

    private static final MemberSelector NO_OP_MEMBER_SELECTOR = member -> true;

    private MemberImpl thisMember;

    private MemberImpl liteMember;

    private MemberImpl dataMember;

    private MemberImpl nonExistingMember;

    private Set<MemberImpl> members;

    @Before
    public void before()
            throws Exception {
        MemberVersion version = MemberVersion.of("3.8.0");
        thisMember
                = new MemberImpl.Builder(new Address("localhost", 5701)).version(version).localMember(true)
                .uuid(newUnsecureUUID()).liteMember(true).build();
        liteMember
                = new MemberImpl.Builder(new Address("localhost", 5702)).version(version).uuid(newUnsecureUUID())
                .liteMember(true).build();
        dataMember
                = new MemberImpl.Builder(new Address("localhost", 5704)).version(version).uuid(newUnsecureUUID()).build();
        nonExistingMember
                = new MemberImpl.Builder(new Address("localhost", 5705)).version(version).uuid(newUnsecureUUID()).build();

        members = createMembers();
    }

    private Set<MemberImpl> createMembers() {
        Set<MemberImpl> members = new LinkedHashSet<MemberImpl>();
        members.add(liteMember);
        members.add(thisMember);
        members.add(dataMember);
        return members;
    }

    @Test
    public void testSizeWhenAllSelected() {
        MemberSelectingCollection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                NO_OP_MEMBER_SELECTOR);
        assertEquals(3, collection.size());
    }

    @Test
    public void testContainsWhenAllSelected() {
        MemberSelectingCollection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                NO_OP_MEMBER_SELECTOR);
        assertContains(collection, liteMember);
        assertContains(collection, thisMember);
        assertContains(collection, dataMember);
    }

    // ################ IS EMPTY ################

    @Test
    public void testIsEmptyWhenNoMemberIsSelected() {
        members.remove(dataMember);
        MemberSelectingCollection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(DATA_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertTrue(collection.isEmpty());
    }

    @Test
    public void testIsEmptyWhenLiteMembersSelectedAndNoLocalMember() {
        members.remove(liteMember);
        members.remove(dataMember);
        MemberSelectingCollection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertTrue(collection.isEmpty());
    }

    // ################ CONTAINS ################

    @Test
    public void testContainsThisMemberWhenLiteMembersSelected() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        assertContains(collection, thisMember);
    }

    @Test
    public void testDoesNotContainThisMemberWhenDataMembersSelected() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, DATA_MEMBER_SELECTOR);
        assertFalse(collection.contains(thisMember));
    }

    @Test
    public void testDoesNotContainThisMemberWhenLiteMembersSelectedAndNoLocalMember() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertFalse(collection.contains(thisMember));
    }

    @Test
    public void testDoesNotContainThisMemberDataMembersSelectedAndNoLocalMember() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(DATA_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertFalse(collection.contains(thisMember));
    }

    @Test
    public void testContainsMatchingMemberWhenLiteMembersSelected() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        assertContains(collection, liteMember);
    }

    @Test
    public void testContainsMatchingMemberWhenLiteMembersSelectedAndNoLocalMember() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertContains(collection, liteMember);
    }

    @Test
    public void testDoesNotContainNonMatchingMemberWhenLiteMembersSelected() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        assertFalse(collection.contains(dataMember));
    }

    @Test
    public void testDoesNotContainNonMatchingMemberWhenLiteMembersSelectedAndNoLocalMember() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertFalse(collection.contains(dataMember));
    }

    @Test
    public void testDoesNotContainOtherMemberWhenDataMembersSelected() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, DATA_MEMBER_SELECTOR);
        assertFalse(collection.contains(nonExistingMember));
    }

    @Test
    public void testDoesNotContainOtherMemberWhenLiteMembersSelected() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        assertFalse(collection.contains(nonExistingMember));
    }

    // ################ CONTAINS ALL ################

    @Test
    public void testContainsAllWhenLiteMembersSelected() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        assertContainsAll(collection, asList(thisMember, liteMember));
    }

    @Test
    public void testDoesNotContainAllWhenLiteMembersSelectedAndNoLocalMember() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertNotContainsAll(collection, asList(thisMember, liteMember));
    }

    @Test
    public void testDoesNotContainNonMatchingMemberTypesWhenLiteMembersSelected() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        assertNotContainsAll(collection, asList(thisMember, dataMember));
    }

    // ################ SIZE ################

    @Test
    public void testSizeWhenThisLiteMembersSelected() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        assertEquals(2, collection.size());
    }

    @Test
    public void testSizeWhenLiteMembersSelectedAndNoLocalMember() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertEquals(1, collection.size());
    }

    @Test
    public void testSizeWhenDataMembersSelectedAndNoLocalMember() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(DATA_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertEquals(1, collection.size());
    }

    @Test
    public void testSizeWhenDataMembersSelected() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, DATA_MEMBER_SELECTOR);
        assertEquals(1, collection.size());
    }

    // ################ TO ARRAY ################

    @Test
    public void testToArrayWhenLiteMembersSelected() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        Object[] array = collection.toArray();

        assertArray(collection, array);
    }

    @Test
    public void testToArrayWhenLiteMembersSelected2() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        Object[] array = new Object[collection.size()];
        collection.toArray(array);

        assertArray(collection, array);
    }

    @Test
    public void testToArrayWhenLiteMembersSelectedAndNoLocalMember() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        Object[] array = collection.toArray();

        assertArray(collection, array);
    }

    @Test
    public void testToArrayWhenLiteMembersSelectedAndNoLocalMember2() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        Object[] array = new Object[collection.size()];
        collection.toArray(array);

        assertArray(collection, array);
    }

    @Test
    public void testToArrayWhenLiteMembersFilteredAndNoLocalMember3() {
        Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        Object[] array = new Object[0];
        array = collection.toArray(array);

        assertArray(collection, array);
    }

    private void assertArray(Collection<MemberImpl> collection, Object[] array) {
        int i = 0;
        for (MemberImpl member : collection) {
            assertEquals(member, array[i++]);
        }
    }
}
