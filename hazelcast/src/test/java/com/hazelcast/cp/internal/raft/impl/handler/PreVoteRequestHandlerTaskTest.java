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

package com.hazelcast.cp.internal.raft.impl.handler;


import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static com.hazelcast.test.HazelcastTestSupport.assumeNotZing;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PreVoteRequestHandlerTaskTest {
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
        assumeNotZing();
        when(raftNode.getLogger(any())).thenReturn(logger);
        when(raftNode.state()).thenReturn(raftState);
        when(raftState.log()).thenReturn(raftLog);

        when(raftNode.getLocalMember()).thenReturn(localEndpoint);
    }

    @Test
    public void voteNotGranted_whenRequestTermIsSmallerThanCurrentTerm() {
        when(raftState.term()).thenReturn(5);

        RaftEndpoint candidate = mock(RaftEndpoint.class);
        PreVoteRequest req = new PreVoteRequest(candidate, 4, 1, 1L, false);

        new PreVoteRequestHandlerTask(raftNode, req).run();

        verify(raftNode).send(
                ArgumentMatchers.<PreVoteResponse>argThat(r ->
                        r.voter().equals(localEndpoint)
                                && r.term() == 5
                                && !r.granted()
                ),
                any()
        );
        verify(raftNode, never()).autoStepDownWhenLeaderForGroup();
    }

    @Test
    public void voteNotGranted_whenLocalLastLogTermIsGreater() {
        when(raftState.term()).thenReturn(1);
        when(raftNode.isLeaderAvailable()).thenReturn(false);
        when(raftLog.lastLogOrSnapshotTerm()).thenReturn(2);
        when(raftLog.lastLogOrSnapshotIndex()).thenReturn(10L);

        RaftEndpoint candidate = mock(RaftEndpoint.class);
        PreVoteRequest req = new PreVoteRequest(candidate, 1, 1, 10L, false);

        new PreVoteRequestHandlerTask(raftNode, req).run();

        verify(raftNode).send(
                ArgumentMatchers.<PreVoteResponse>argThat(r ->
                        r.voter().equals(localEndpoint)
                                && r.term() == 1
                                && !r.granted()
                ),
                any()
        );
    }

    @Test
    public void voteNotGranted_whenLogsSameTermButLocalIndexGreater() {
        when(raftState.term()).thenReturn(1);
        when(raftNode.isLeaderAvailable()).thenReturn(false);
        when(raftLog.lastLogOrSnapshotTerm()).thenReturn(1);
        when(raftLog.lastLogOrSnapshotIndex()).thenReturn(5L);

        RaftEndpoint candidate = mock(RaftEndpoint.class);
        PreVoteRequest req = new PreVoteRequest(candidate, 1, 1, 4L, false);

        new PreVoteRequestHandlerTask(raftNode, req).run();

        verify(raftNode).send(
                ArgumentMatchers.<PreVoteResponse>argThat(r ->
                        r.voter().equals(localEndpoint)
                                && r.term() == 1
                                && !r.granted()
                ),
                any()
        );
    }

    @Test
    public void voteNotGranted_whenLeaderIsAvailable() {
        when(raftState.term()).thenReturn(1);
        when(raftNode.isLeaderAvailable()).thenReturn(true);

        RaftEndpoint candidate = mock(RaftEndpoint.class);
        PreVoteRequest req = new PreVoteRequest(candidate, 1, 1, 1L, false);

        new PreVoteRequestHandlerTask(raftNode, req).run();

        verify(raftNode).send(
                ArgumentMatchers.<PreVoteResponse>argThat(r ->
                        r.voter().equals(localEndpoint)
                                && r.term() == 1
                                && !r.granted()
                ),
                any()
        );
        verify(raftState, never()).log();
    }

    @Test
    public void voteGranted_whenCandidateLogIsNotWorse() {
        when(raftState.term()).thenReturn(1);
        when(raftNode.isLeaderAvailable()).thenReturn(false);
        when(raftLog.lastLogOrSnapshotTerm()).thenReturn(1);
        when(raftLog.lastLogOrSnapshotIndex()).thenReturn(1L);

        RaftEndpoint candidate = mock(RaftEndpoint.class);
        PreVoteRequest req = new PreVoteRequest(candidate, 2, 1, 2L, false);

        new PreVoteRequestHandlerTask(raftNode, req).run();

        verify(raftNode).send(
                ArgumentMatchers.<PreVoteResponse>argThat(r ->
                        r.voter().equals(localEndpoint)
                                && r.term() == 2
                                && r.granted()
                ),
                any()
        );
    }

    @Test
    public void voteGrantedForAutoStepDownCandidateWhenLocalAlsoAutoStepDown_andLogsEqual() {
        when(raftState.term()).thenReturn(1);
        when(raftNode.isLeaderAvailable()).thenReturn(false);
        when(raftLog.lastLogOrSnapshotTerm()).thenReturn(1);
        when(raftLog.lastLogOrSnapshotIndex()).thenReturn(1L);

        when(raftNode.autoStepDownWhenLeaderForGroup()).thenReturn(true);

        RaftEndpoint candidate = mock(RaftEndpoint.class);
        PreVoteRequest req = new PreVoteRequest(candidate, 1, 1, 1L, true);

        new PreVoteRequestHandlerTask(raftNode, req).run();

        verify(raftNode).send(
                ArgumentMatchers.<PreVoteResponse>argThat(r ->
                        r.voter().equals(localEndpoint)
                                && r.term() == 1
                                && r.granted()
                ),
                any()
        );
    }


    @Test
    public void voteNotGrantedForAutoStepDownMemberWhenLogsEqual() {
        when(raftState.term()).thenReturn(1);
        when(raftNode.isLeaderAvailable()).thenReturn(false);

        when(raftLog.lastLogOrSnapshotIndex()).thenReturn(1L);
        when(raftLog.lastLogOrSnapshotTerm()).thenReturn(1);

        when(raftNode.autoStepDownWhenLeaderForGroup()).thenReturn(false);

        RaftEndpoint candidate = mock(RaftEndpoint.class);

        int nextTerm = 1;
        int lastLogTerm = 1;
        long lastLogIndex = 1;

        PreVoteRequest req =
                new PreVoteRequest(candidate, nextTerm, lastLogTerm, lastLogIndex, true);

        new PreVoteRequestHandlerTask(raftNode, req).run();
        verify(raftNode).send(
                ArgumentMatchers.<PreVoteResponse>argThat(response ->
                        response != null
                                && response.voter().equals(localEndpoint)
                                && response.term() == req.nextTerm()
                                && !response.granted()
                ),
                any()
        );
    }
}
