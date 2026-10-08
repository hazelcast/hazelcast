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

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.exception.CPLeaderSteppingDownException;
import com.hazelcast.cp.exception.CannotReplicateException;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.log.LogEntry;
import com.hazelcast.cp.internal.raft.impl.log.RaftLog;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.UUID;

import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.ACTIVE;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.INITIAL;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.STEPPED_DOWN;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.TERMINATED;
import static com.hazelcast.cp.internal.raft.impl.RaftRole.FOLLOWER;
import static com.hazelcast.cp.internal.raft.impl.RaftRole.LEADER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicateTaskTest {

    @Mock
    private RaftNodeImpl raftNode;
    @Mock
    private Object operation;
    @Mock
    private InternalCompletableFuture resultFuture;
    @Mock
    private ILogger logger;
    @Mock
    private RaftState raftState;
    @Mock
    private RaftLog raftLog;
    @Mock
    private RaftEndpoint localEndpoint;
    private final UUID locaUuid = UUID.randomUUID();

    private final CPGroupId groupId = new RaftGroupId("test", 1, 1);

    @Before
    public void setUp() {
        when(raftNode.getLogger(any())).thenReturn(logger);
        when(raftNode.state()).thenReturn(raftState);
        when(raftState.log()).thenReturn(raftLog);
        when(raftNode.getGroupId()).thenReturn(groupId);
        when(localEndpoint.getUuid()).thenReturn(locaUuid);

        when(raftNode.getLocalMember()).thenReturn(localEndpoint);
    }

    @Test
    public void cannotReplicateWhenRaftNodeIsNotInitialized() {
        when(raftNode.getStatus()).thenReturn(INITIAL);
        ReplicateTask task = new ReplicateTask(raftNode, operation, resultFuture);
        task.run();
        verify(resultFuture).completeExceptionally(argThat(e ->
                e instanceof CannotReplicateException
                        && "Cannot replicate new operations for now".equals(e.getMessage())
        ));
        verifyNoInteractions(raftLog);
    }

    @Test
    public void cannotReplicateWhenRaftNodeIsTerminated() {
        when(raftNode.getStatus()).thenReturn(TERMINATED);
        ReplicateTask task = new ReplicateTask(raftNode, operation, resultFuture);
        task.run();
        verify(resultFuture).completeExceptionally(argThat(e ->
                e instanceof NotLeaderException
                        && "localEndpoint is not LEADER of CPGroupId{name='test', seed=1, groupId=1}. Known leader is: N/A"
                                .equals(e.getMessage())
        ));
        verifyNoInteractions(raftLog);
    }

    @Test
    public void cannotReplicateWhenRaftNodeIsSteppedDown() {
        when(raftNode.getStatus()).thenReturn(STEPPED_DOWN);
        ReplicateTask task = new ReplicateTask(raftNode, operation, resultFuture);
        task.run();
        verify(resultFuture).completeExceptionally(argThat(e ->
                e instanceof NotLeaderException
                        && "localEndpoint is not LEADER of CPGroupId{name='test', seed=1, groupId=1}. Known leader is: N/A".equals(e.getMessage())
        ));
        verifyNoInteractions(raftLog);
    }


    @Test
    public void cannotReplicateWhenRaftNodeIsNotLeader() {
        when(raftNode.getStatus()).thenReturn(ACTIVE);
        when(raftState.role()).thenReturn(FOLLOWER);
        ReplicateTask task = new ReplicateTask(raftNode, operation, resultFuture);
        task.run();
        verify(resultFuture).completeExceptionally(argThat(e ->
                e instanceof NotLeaderException
                        && "localEndpoint is not LEADER of CPGroupId{name='test', seed=1, groupId=1}. Known leader is: N/A".equals(e.getMessage())
        ));

        verifyNoInteractions(raftLog);
    }

    @Test
    public void cannotReplicateWhenRaftNodeIsAbdicatingLeadershipForGroup() {
        when(raftNode.getStatus()).thenReturn(ACTIVE);
        when(raftState.role()).thenReturn(LEADER);
        when(raftNode.autoStepDownWhenLeaderForGroup()).thenReturn(true);
        ReplicateTask task = new ReplicateTask(raftNode, operation, resultFuture);
        task.run();
        verify(resultFuture).completeExceptionally(argThat(e ->
                e instanceof CPLeaderSteppingDownException
                        && "Service unavailable, leader is auto stepping down".equals(e.getMessage())
        ));

        verifyNoInteractions(raftLog);
    }


    @Test
    public void cannotReplicateWhenRaftNodeCannotReplicateNewEntry() {
        when(raftNode.getStatus()).thenReturn(ACTIVE);
        when(raftState.role()).thenReturn(LEADER);
        when(raftNode.canReplicateNewEntry(operation)).thenReturn(false);
        ReplicateTask task = new ReplicateTask(raftNode, operation, resultFuture);
        task.run();
        verify(resultFuture).completeExceptionally(argThat(e ->
                e instanceof CannotReplicateException
                        && "Cannot replicate new operations for now".equals(e.getMessage())));
        verifyNoInteractions(raftLog);
    }

    @Test
    public void cannotReplicateWhenRaftLogDoesNotHaveEnoughCapacity() {
        when(raftNode.getStatus()).thenReturn(ACTIVE);
        when(raftState.role()).thenReturn(LEADER);
        when(raftNode.canReplicateNewEntry(operation)).thenReturn(true);
        when(raftLog.checkAvailableCapacity(1)).thenReturn(false);
        ReplicateTask task = new ReplicateTask(raftNode, operation, resultFuture);
        task.run();
        verify(resultFuture).completeExceptionally(argThat(e ->
                e instanceof IllegalStateException
                        && "Not enough capacity in RaftLog!".equals(e.getMessage())
        ));

        verify(raftLog, never()).appendEntries(any());
    }

    @Test
    public void testReplicateTaskSucceeds() {
        when(raftNode.getStatus()).thenReturn(ACTIVE);
        when(raftState.role()).thenReturn(LEADER);
        when(raftNode.canReplicateNewEntry(operation)).thenReturn(true);
        when(raftLog.checkAvailableCapacity(1)).thenReturn(true);
        when(raftLog.lastLogOrSnapshotIndex()).thenReturn(0L);
        when(raftState.term()).thenReturn(1);
        ReplicateTask task = new ReplicateTask(raftNode, operation, resultFuture);
        task.run();


        verify(raftNode).registerFuture(1, resultFuture);
        verify(raftLog).appendEntries(new LogEntry(1, 1, operation));
        verify(raftNode).broadcastAppendRequest();
    }
}
