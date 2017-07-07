package com.hazelcast.internal.util.iterator;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RestartingMemberIteratorTest extends HazelcastTestSupport {

    private ClusterService mockClusterService;

    @Before
    public void setUp() {
        Set<Member> memberSet = new LinkedHashSet<Member>();
        mockClusterService = mock(ClusterService.class);
        when(mockClusterService.getMembers())
                .thenReturn(memberSet);
    }

    private Member addClusterMember() {
        Set<Member> currentMemberSet = mockClusterService.getMembers();
        Set<Member> newMemberSet = new LinkedHashSet<Member>(currentMemberSet);

        Member mockMember = mock(Member.class);
        newMemberSet.add(mockMember);

        when(mockClusterService.getMembers())
                .thenReturn(newMemberSet);
        return mockMember;
    }

    @Test
    public void testIteratorOverSingleEntry() {
        int maxRetries = 0;
        Member mockMember = addClusterMember();

        RestartingMemberIterator iterator = new RestartingMemberIterator(mockClusterService, maxRetries);
        assertTrue(iterator.hasNext());

        Member member = iterator.next();
        assertSame(mockMember, member);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testRestart() {
        int maxRetries = 1;
        Member mockMember = addClusterMember();

        RestartingMemberIterator iterator = new RestartingMemberIterator(mockClusterService, maxRetries);
        assertTrue(iterator.hasNext());

        Member member = iterator.next();
        assertSame(mockMember, member);

        //topologoy change -> this should restart iteration
        Member anotherMockMember = addClusterMember();

        member = iterator.next();
        assertSame(mockMember, member);
        member = iterator.next();
        assertSame(anotherMockMember, member);
        assertFalse(iterator.hasNext());
    }

    @Test(expected = HazelcastException.class)
    public void testRestart_withMaxRetriesExhausted_nextThrowsException() {
        int maxRetries = 0;
        addClusterMember();

        RestartingMemberIterator iterator = new RestartingMemberIterator(mockClusterService, maxRetries);
        // start iterating
        iterator.next();

        // topologoy change -> this should restart iteration, but there are no retry attempt left
        addClusterMember();

        // this has to throw the Exception
        iterator.next();
    }

    @Test(expected = HazelcastException.class)
    public void testRestart_withMaxRetriesExhausted_hasNextThrowsException() {
        int maxRetries = 0;
        addClusterMember();

        RestartingMemberIterator iterator = new RestartingMemberIterator(mockClusterService, maxRetries);
        iterator.next();

        addClusterMember();

        iterator.hasNext();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void removeThrowUOE() {
        int maxRetries = 0;
        addClusterMember();

        RestartingMemberIterator iterator = new RestartingMemberIterator(mockClusterService, maxRetries);
        iterator.next();
        iterator.remove();
    }

    @Test(expected = NoSuchElementException.class)
    public void give() {
        int maxRetries = 0;
        addClusterMember();

        RestartingMemberIterator iterator = new RestartingMemberIterator(mockClusterService, maxRetries);
        iterator.next();

        //this should throw NoSuchElementException
        iterator.next();
    }

}