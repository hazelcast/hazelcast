package com.hazelcast.internal.cluster.impl;

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
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
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberSelectingCollectionTest {

    private static final MemberSelector NO_OP_MEMBER_SELECTOR = new MemberSelector() {
        @Override
        public boolean select(Member member) {
            return true;
        }
    };

    private MemberImpl thisMember;

    private MemberImpl liteMember;

    private MemberImpl dataMember;

    private MemberImpl nonExistingMember;

    private Set<MemberImpl> members;

    @Before
    public void before()
            throws Exception {
        thisMember = new MemberImpl(new Address("localhost", 5701), true, true);
        liteMember = new MemberImpl(new Address("localhost", 5702), false, true);
        dataMember = new MemberImpl(new Address("localhost", 5704), false, false);
        nonExistingMember = new MemberImpl(new Address("localhost", 5705), false, false);

        members = createMembers();
    }

    private Set<MemberImpl> createMembers() {
        final Set<MemberImpl> members = new LinkedHashSet<MemberImpl>();
        members.add(liteMember);
        members.add(thisMember);
        members.add(dataMember);
        return members;
    }

    @Test
    public void testSizeWhenAllSelected() {
        final MemberSelectingCollection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                NO_OP_MEMBER_SELECTOR);
        assertEquals(3, collection.size());
    }

    @Test
    public void testContainsWhenAllSelected() {
        final MemberSelectingCollection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                NO_OP_MEMBER_SELECTOR);
        assertTrue(collection.contains(liteMember));
        assertTrue(collection.contains(thisMember));
        assertTrue(collection.contains(dataMember));
    }

    // ################ IS EMPTY ################

    @Test
    public void testIsEmptyWhenNoMemberIsSelected() {
        members.remove(dataMember);
        final MemberSelectingCollection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(DATA_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertTrue(collection.isEmpty());
    }

    @Test
    public void testIsEmptyWhenLiteMembersSelectedAndNoLocalMember() {
        members.remove(liteMember);
        members.remove(dataMember);
        final MemberSelectingCollection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertTrue(collection.isEmpty());
    }

    // ################ CONTAINS ################

    @Test
    public void testContainsThisMemberWhenLiteMembersSelected() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        assertTrue(collection.contains(thisMember));
    }

    @Test
    public void testDoesNotContainThisMemberWhenDataMembersSelected() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, DATA_MEMBER_SELECTOR);
        assertFalse(collection.contains(thisMember));
    }

    @Test
    public void testDoesNotContainThisMemberWhenLiteMembersSelectedAndNoLocalMember() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertFalse(collection.contains(thisMember));
    }

    @Test
    public void testDoesNotContainThisMemberDataMembersSelectedAndNoLocalMember() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(DATA_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertFalse(collection.contains(thisMember));
    }

    @Test
    public void testContainsMatchingMemberWhenLiteMembersSelected() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        assertTrue(collection.contains(liteMember));
    }

    @Test
    public void testContainsMatchingMemberWhenLiteMembersSelectedAndNoLocalMember() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertTrue(collection.contains(liteMember));
    }

    @Test
    public void testDoesNotContainNonMatchingMemberWhenLiteMembersSelected() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        assertFalse(collection.contains(dataMember));
    }

    @Test
    public void testDoesNotContainNonMatchingMemberWhenLiteMembersSelectedAndNoLocalMember() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertFalse(collection.contains(dataMember));
    }

    @Test
    public void testDoesNotContainOtherMemberWhenDataMembersSelected() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, DATA_MEMBER_SELECTOR);
        assertFalse(collection.contains(nonExistingMember));
    }

    @Test
    public void testDoesNotContainOtherMemberWhenLiteMembersSelected() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        assertFalse(collection.contains(nonExistingMember));
    }

    // ################ CONTAINS ALL ################

    @Test
    public void testContainsAllWhenLiteMembersSelected() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        assertTrue(collection.containsAll(asList(thisMember, liteMember)));
    }

    @Test
    public void testDoesNotContainAllWhenLiteMembersSelectedAndNoLocalMember() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertFalse(collection.containsAll(asList(thisMember, liteMember)));
    }

    @Test
    public void testDoesNotContainNonMatchingMemberTypesWhenLiteMembersSelected() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        assertFalse(collection.containsAll(asList(thisMember, dataMember)));
    }

    // ################ SIZE ################

    @Test
    public void testSizeWhenThisLiteMembersSelected() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        assertEquals(2, collection.size());
    }

    @Test
    public void testSizeWhenLiteMembersSelectedAndNoLocalMember() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertEquals(1, collection.size());
    }

    @Test
    public void testSizeWhenDataMembersSelectedAndNoLocalMember() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(DATA_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        assertEquals(1, collection.size());
    }

    @Test
    public void testSizeWhenDataMembersSelected() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, DATA_MEMBER_SELECTOR);
        assertEquals(1, collection.size());
    }

    // ################ TO ARRAY ################

    @Test
    public void testToArrayWhenLiteMembersSelected() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        final Object[] array = collection.toArray();

        assertArray(collection, array);
    }

    @Test
    public void testToArrayWhenLiteMembersSelected2() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members, LITE_MEMBER_SELECTOR);
        final Object[] array = new Object[collection.size()];
        collection.toArray(array);

        assertArray(collection, array);
    }

    @Test
    public void testToArrayWhenLiteMembersSelectedAndNoLocalMember() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        final Object[] array = collection.toArray();

        assertArray(collection, array);
    }

    @Test
    public void testToArrayWhenLiteMembersSelectedAndNoLocalMember2() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
                and(LITE_MEMBER_SELECTOR, NON_LOCAL_MEMBER_SELECTOR));
        final Object[] array = new Object[collection.size()];
        collection.toArray(array);

        assertArray(collection, array);
    }

    @Test
    public void testToArrayWhenLiteMembersFilteredAndNoLocalMember3() {
        final Collection<MemberImpl> collection = new MemberSelectingCollection<MemberImpl>(members,
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
