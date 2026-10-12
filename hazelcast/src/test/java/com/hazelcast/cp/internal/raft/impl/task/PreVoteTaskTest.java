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

package com.hazelcast.cp.internal.raft.impl.task;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PreVoteTaskTest {

    @Mock
    private RaftNodeImpl raftNode;
    @Mock
    private ILogger logger;
    @Mock
    private RaftState raftState;
    @Mock
    private RaftLog raftLog;
    @Mock
    private RaftEndpoint localEndpoint;

    @Before
    public void setUp() {
        doReturn(logger).when(raftNode).getLogger(any());

        when(raftNode.state()).thenReturn(raftState);
        when(raftState.log()).thenReturn(raftLog);
        when(raftNode.getLocalMember()).thenReturn(localEndpoint);
    }

    @Test
    public void testPreVoteReturnsIfLeaderIsNotNull() {
        PreVoteTask preVoteTask = new PreVoteTask(raftNode, 0);
        when(raftState.leader()).thenReturn(localEndpoint);

        preVoteTask.innerRun();
        verify(logger).fine("No new pre-vote phase, we already have a LEADER: %s", localEndpoint);
        verify(raftNode, never()).send(any(PreVoteRequest.class), any(RaftEndpoint.class));
        verify(raftNode, never()).schedule(any(), anyLong());
    }

    @Test
    public void testPreVoteReturnsIfTermIsNotEqual() {
        PreVoteTask preVoteTask = new PreVoteTask(raftNode, 0);
        when(raftState.term()).thenReturn(1);
        when(raftState.leader()).thenReturn(null);
        preVoteTask.innerRun();
        verify(logger).fine("No new pre-vote phase for term= %s because of new term: %s", 0, 1);
        verify(raftNode, never()).send(any(PreVoteRequest.class), any(RaftEndpoint.class));
        verify(raftNode, never()).schedule(any(), anyLong());
    }

    @Test
    public void testPreVoteReturnsForTermZeroIfLeaderStepsDown() {
        PreVoteTask preVoteTask = new PreVoteTask(raftNode, 0);
        when(raftNode.autoStepDownWhenLeaderForGroup()).thenReturn(true);
        when(raftState.term()).thenReturn(0);
        when(raftState.leader()).thenReturn(null);
        preVoteTask.innerRun();
        verify(logger).info("This node auto steps down from leadership, not participating in pre-vote for term 0");
        verify(raftNode, never()).send(any(PreVoteRequest.class), any(RaftEndpoint.class));
        verify(raftNode, never()).schedule(any(), anyLong());
    }

    @Test
    public void testPreVoteReturnsIfRemoteMembersIsEmpty() {
        PreVoteTask preVoteTask = new PreVoteTask(raftNode, 0);
        when(raftState.term()).thenReturn(0);
        when(raftState.leader()).thenReturn(null);
        when(raftState.remoteMembers()).thenReturn(Collections.EMPTY_SET);
        preVoteTask.innerRun();
        verify(logger).fine("Remote members is empty. No need for pre-voting.");
        verify(raftNode, never()).send(any(PreVoteRequest.class), any(RaftEndpoint.class));
        verify(raftNode, never()).schedule(any(), anyLong());
    }

    @Test
    public void testPreVoteSendsRequestAndSchedulesTimeout() {
        PreVoteTask preVoteTask = new PreVoteTask(raftNode, 0);
        when(raftState.term()).thenReturn(0);
        when(raftState.leader()).thenReturn(null);
        when(raftState.remoteMembers()).thenReturn(Collections.singleton(localEndpoint));
        when(raftLog.lastLogOrSnapshotTerm()).thenReturn(1);
        when(raftLog.lastLogOrSnapshotIndex()).thenReturn(2L);

        preVoteTask.innerRun();

        verify(logger).info("Pre-vote started for next term: 1, last log index: 2, last log term: 1");
        verify(raftNode).send(
                argThat((PreVoteRequest request) ->
                        request.nextTerm() == 1
                                && request.lastLogIndex() == 2L
                                && request.lastLogTerm() == 1L),
                any(RaftEndpoint.class)
        );

        verify(raftNode).schedule(any(), anyLong());
    }
}
