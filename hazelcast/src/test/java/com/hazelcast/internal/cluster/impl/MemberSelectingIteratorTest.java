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

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.LITE_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.cluster.memberselector.MemberSelectors.and;
import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberSelectingIteratorTest extends HazelcastTestSupport {

    private MemberImpl thisMember;

    private MemberImpl matchingMember;

    private MemberImpl matchingMember2;

    private MemberImpl nonMatchingMember;

    @Before
    public void before()
            throws Exception {
        MemberVersion version = new MemberVersion(BuildInfoProvider.getBuildInfo().getVersion());
        thisMember = new MemberImpl.Builder(new Address("localhost", 5701)).version(version).localMember(true)
                .uuid(newUnsecureUUID()).liteMember(true).build();
        matchingMember = new MemberImpl.Builder(new Address("localhost", 5702)).version(version).uuid(newUnsecureUUID())
                .liteMember(true).build();
        matchingMember2 = new MemberImpl.Builder(new Address("localhost", 5703)).version(version).uuid(newUnsecureUUID())
                .liteMember(true).build();
        nonMatchingMember = new MemberImpl.Builder(new Address("localhost", 5704)).version(version).uuid(newUnsecureUUID())
                .build();
    }

    private Set<MemberImpl> createMembers() {
        Set<MemberImpl> members = new LinkedHashSet<>();
        members.add(thisMember);
        members.add(matchingMember);
        members.add(nonMatchingMember);
        members.add(matchingMember2);
        return members;
    }

    @Test
    public void testSelectingLiteMembersWithThisAddress() {
        Set<MemberImpl> members = createMembers();
        Iterator<MemberImpl> iterator = new MemberSelectingCollection<>(members, LITE_MEMBER_SELECTOR).iterator();
        Set<MemberImpl> filteredMembers = new HashSet<>();

        while (iterator.hasNext()) {
            filteredMembers.add(iterator.next());
        }

        assertEquals(3, filteredMembers.size());
        assertContains(filteredMembers, thisMember);
        assertContains(filteredMembers, matchingMember);
        assertContains(filteredMembers, matchingMember2);
    }

    @Test
    public void testSelectingLiteMembersWithoutThisAddress() {
        Set<MemberImpl> members = createMembers();
        Iterator<MemberImpl> iterator = new MemberSelectingCollection<>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR)).iterator();
        Set<MemberImpl> filteredMembers = new HashSet<>();

        while (iterator.hasNext()) {
            filteredMembers.add(iterator.next());
        }

        assertEquals(2, filteredMembers.size());
        assertContains(filteredMembers, matchingMember);
        assertContains(filteredMembers, matchingMember2);
    }

    @Test
    public void testSelectingMembersWithThisAddress() {
        Set<MemberImpl> members = createMembers();
        Iterator<MemberImpl> iterator = new MemberSelectingCollection<>(members, DATA_MEMBER_SELECTOR).iterator();
        Set<MemberImpl> filteredMembers = new HashSet<>();

        while (iterator.hasNext()) {
            filteredMembers.add(iterator.next());
        }

        assertEquals(1, filteredMembers.size());
        assertContains(filteredMembers, nonMatchingMember);
    }

    @Test
    public void testSelectingMembersWithoutThisAddress() {
        Set<MemberImpl> members = createMembers();
        Iterator<MemberImpl> iterator = new MemberSelectingCollection<>(members,
                and(DATA_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR)).iterator();
        Set<MemberImpl> filteredMembers = new HashSet<>();

        while (iterator.hasNext()) {
            filteredMembers.add(iterator.next());
        }

        assertEquals(1, filteredMembers.size());
        assertContains(filteredMembers, nonMatchingMember);
    }

    @Test
    public void testHasNextCalledTwice() {
        Set<MemberImpl> members = createMembers();
        Iterator<MemberImpl> iterator = new MemberSelectingCollection<>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR)).iterator();

        while (iterator.hasNext()) {
            iterator.hasNext();
            iterator.next();
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void testIterationFailsAfterConsumed() {
        Set<MemberImpl> members = createMembers();
        Iterator<MemberImpl> iterator = new MemberSelectingCollection<>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR)).iterator();

        while (iterator.hasNext()) {
            iterator.next();
        }

        iterator.next();
    }
}
