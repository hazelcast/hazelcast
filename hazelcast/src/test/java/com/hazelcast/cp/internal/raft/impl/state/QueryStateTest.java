/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.raft.impl.state;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cp.internal.raft.impl.RaftUtil.newRaftMember;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryStateTest {

    private static final int MAJORITY = 2;

    private QueryState state;
    private RaftEndpoint follower;

    @Before
    public void setup() {
        state = new QueryState();
        follower = newRaftMember(5001);
    }

    @Test
    public void test_initialState() {
        assertEquals(0, state.queryCount());
        assertEquals(0, state.queryRound());
        assertTrue(state.operations().isEmpty());
        assertFalse(state.isMajorityAcked(1, MAJORITY));
    }

    @Test
    public void addQuery_startsNewRound_whenThereIsNoWaitingQuery() {
        assertEquals(1, addQuery(1));

        assertEquals(1, state.queryRound());
    }

    @Test
    public void addQuery_doesNotStartNewRound_whileQueriesAreWaiting() {
        addQuery(1);

        assertEquals(2, addQuery(1));

        // The round is only started by the first query, hence the waiting queries must be
        // disposed of rather than left behind, otherwise a later query would join their
        // finished round and could be run on the acknowledgements collected for it.
        assertEquals(1, state.queryRound());
    }

    @Test
    public void isMajorityAcked_countsTheLeaderItself() {
        addQuery(1);

        assertFalse(state.isMajorityAcked(1, MAJORITY));
        assertTrue(state.tryAck(state.queryRound(), follower));
        assertTrue(state.isMajorityAcked(1, MAJORITY));
    }

    @Test
    public void fail_completesWaitingQueriesExceptionally() {
        InternalCompletableFuture<Object> first = new InternalCompletableFuture<>();
        InternalCompletableFuture<Object> second = new InternalCompletableFuture<>();
        state.addQuery(1, "first", first);
        state.addQuery(1, "second", second);
        RuntimeException failure = new RuntimeException("expected");

        state.fail(failure);

        assertTrue(first.isCompletedExceptionally());
        assertTrue(second.isCompletedExceptionally());
        // joinInternal() re-creates the exception to attach the local stack trace, so the
        // message is compared rather than the instance
        assertEquals(failure.getMessage(), assertThrows(RuntimeException.class, first::joinInternal).getMessage());
        assertEquals(failure.getMessage(), assertThrows(RuntimeException.class, second::joinInternal).getMessage());
    }

    @Test
    public void fail_clearsWaitingQueriesAndAcks() {
        addQuery(1);
        long round = state.queryRound();
        state.tryAck(round, follower);

        state.fail(new RuntimeException("expected"));

        assertEquals(0, state.queryCount());
        assertTrue(state.operations().isEmpty());
        // The acknowledgement of the failed round is gone, hence it cannot be counted again
        assertFalse(state.tryAck(round, follower));
        assertEquals(1, addQuery(2));
        assertEquals(round + 1, state.queryRound());
        assertFalse(state.isMajorityAcked(2, MAJORITY));
    }

    @Test
    public void fail_isNoOp_whenThereIsNoWaitingQuery() {
        state.fail(new RuntimeException("expected"));

        assertEquals(0, state.queryCount());
        assertEquals(0, state.queryRound());
    }

    private int addQuery(long commitIndex) {
        return state.addQuery(commitIndex, "query at " + commitIndex, new InternalCompletableFuture<>());
    }
}
