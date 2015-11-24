package com.hazelcast.cluster.impl;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberSelectingIteratorTest {

    private MemberImpl thisMember;

    private MemberImpl matchingMember;

    private MemberImpl matchingMember2;

    private MemberImpl nonMatchingMember;

    @Before
    public void before()
            throws Exception {
        thisMember = new MemberImpl(new Address("localhost", 5701), true, true);
        matchingMember = new MemberImpl(new Address("localhost", 5702), false, true);
        matchingMember2 = new MemberImpl(new Address("localhost", 5703), false, true);
        nonMatchingMember = new MemberImpl(new Address("localhost", 5704), false, false);
    }

    private Set<MemberImpl> createMembers() {
        final Set<MemberImpl> members = new LinkedHashSet<MemberImpl>();
        members.add(thisMember);
        members.add(matchingMember);
        members.add(nonMatchingMember);
        members.add(matchingMember2);
        return members;
    }

    @Test
    public void testSelectingLiteMembersWithThisAddress() {
        final Set<MemberImpl> members = createMembers();
        final Iterator<MemberImpl> iterator = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR).iterator();
        final Set<MemberImpl> filteredMembers = new HashSet<MemberImpl>();

        while (iterator.hasNext()) {
            filteredMembers.add(iterator.next());
        }

        assertEquals(3, filteredMembers.size());
        assertTrue(filteredMembers.contains(thisMember));
        assertTrue(filteredMembers.contains(matchingMember));
        assertTrue(filteredMembers.contains(matchingMember2));
    }

    @Test
    public void testSelectingLiteMembersWithoutThisAddress() {
        final Set<MemberImpl> members = createMembers();
        final Iterator<MemberImpl> iterator = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR)).iterator();
        final Set<MemberImpl> filteredMembers = new HashSet<MemberImpl>();

        while (iterator.hasNext()) {
            filteredMembers.add(iterator.next());
        }

        assertEquals(2, filteredMembers.size());
        assertTrue(filteredMembers.contains(matchingMember));
        assertTrue(filteredMembers.contains(matchingMember2));
    }

    @Test
    public void testSelectingMembersWithThisAddress() {
        final Set<MemberImpl> members = createMembers();
        final Iterator<MemberImpl> iterator = new MemberSelectingCollection<MemberImpl>(members, DATA_MEMBER_SELECTOR).iterator();
        final Set<MemberImpl> filteredMembers = new HashSet<MemberImpl>();

        while (iterator.hasNext()) {
            filteredMembers.add(iterator.next());
        }

        assertEquals(1, filteredMembers.size());
        assertTrue(filteredMembers.contains(nonMatchingMember));
    }

    @Test
    public void testSelectingMembersWithoutThisAddress() {
        final Set<MemberImpl> members = createMembers();
        final Iterator<MemberImpl> iterator = new MemberSelectingCollection<MemberImpl>(members,
                and(DATA_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR)).iterator();
        final Set<MemberImpl> filteredMembers = new HashSet<MemberImpl>();

        while (iterator.hasNext()) {
            filteredMembers.add(iterator.next());
        }

        assertEquals(1, filteredMembers.size());
        assertTrue(filteredMembers.contains(nonMatchingMember));
    }

    @Test
    public void testHasNextCalledTwice() {
        final Set<MemberImpl> members = createMembers();
        final Iterator<MemberImpl> iterator = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR)).iterator();

        while (iterator.hasNext()) {
            iterator.hasNext();
            iterator.next();
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void testIterationFailsAfterConsumed() {
        final Set<MemberImpl> members = createMembers();
        final Iterator<MemberImpl> iterator = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR)).iterator();

        while (iterator.hasNext()) {
            iterator.next();
        }

        iterator.next();
    }

}
