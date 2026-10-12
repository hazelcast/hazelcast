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
import com.hazelcast.cp.exception.CannotReplicateException;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.raft.exception.MemberDoesNotExistException;
import com.hazelcast.cp.internal.raft.exception.MismatchingGroupMembersCommitIndexException;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.command.UpdateRaftGroupMembersCmd;
import com.hazelcast.cp.internal.raft.impl.state.RaftGroupMembers;
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

import java.util.Collections;

import static com.hazelcast.cp.internal.raft.MembershipChangeMode.ADD;
import static com.hazelcast.cp.internal.raft.MembershipChangeMode.REMOVE;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.ACTIVE;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.INITIAL;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.STEPPED_DOWN;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.TERMINATED;
import static com.hazelcast.cp.internal.raft.impl.RaftRole.FOLLOWER;
import static com.hazelcast.cp.internal.raft.impl.RaftRole.LEADER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MembershipChangeTaskTest {

    @Mock
    private RaftNodeImpl raftNode;
    @Mock
    private InternalCompletableFuture resultFuture;
    @Mock
    private ILogger logger;
    @Mock
    private RaftState raftState;
    @Mock
    private RaftEndpoint localEndpoint;

    private final CPGroupId groupId = new RaftGroupId("test", 1, 1);

    @Before
    public void setUp() {
        when(raftNode.getLogger(any())).thenReturn(logger);
        when(raftNode.state()).thenReturn(raftState);
        when(raftNode.getGroupId()).thenReturn(groupId);

        when(raftNode.getLocalMember()).thenReturn(localEndpoint);
    }

    @Test
    public void cannotChangeMembershipWhenRaftNodeIsNotInitialized() {
        when(raftNode.getStatus()).thenReturn(INITIAL);
        MembershipChangeTask task = new MembershipChangeTask(raftNode, resultFuture, localEndpoint, ADD);

        task.run();
        verify(resultFuture).completeExceptionally(argThat(e ->
                e instanceof CannotReplicateException
                        && "Cannot replicate new operations for now".equals(e.getMessage())
        ));
        verify(raftNode, never()).createReplicationTask(any(UpdateRaftGroupMembersCmd.class), eq(resultFuture));
    }

    @Test
    public void cannotChangeMembershipWhenRaftNodeIsTerminated() {
        when(raftNode.getStatus()).thenReturn(TERMINATED);
        MembershipChangeTask task = new MembershipChangeTask(raftNode, resultFuture, localEndpoint, ADD);
        task.run();
        verify(resultFuture).completeExceptionally(argThat(e ->
                e instanceof NotLeaderException
                        && "localEndpoint is not LEADER of CPGroupId{name='test', seed=1, groupId=1}. Known leader is: N/A"
                                .equals(e.getMessage())
        ));
        verify(raftNode, never()).createReplicationTask(any(UpdateRaftGroupMembersCmd.class), eq(resultFuture));
    }

    @Test
    public void cannotChangeMembershipWhenRaftNodeIsSteppedDown() {
        when(raftNode.getStatus()).thenReturn(STEPPED_DOWN);
        MembershipChangeTask task = new MembershipChangeTask(raftNode, resultFuture, localEndpoint, ADD);
        task.run();
        verify(resultFuture).completeExceptionally(argThat(e ->
                e instanceof NotLeaderException
                        && "localEndpoint is not LEADER of CPGroupId{name='test', seed=1, groupId=1}. Known leader is: N/A".equals(e.getMessage())
        ));
        verify(raftNode, never()).createReplicationTask(any(UpdateRaftGroupMembersCmd.class), eq(resultFuture));
    }

    @Test
    public void cannotChangeMembershipWhenRaftNodeIsNotLeader() {
        when(raftNode.getStatus()).thenReturn(ACTIVE);
        when(raftState.role()).thenReturn(FOLLOWER);
        MembershipChangeTask task = new MembershipChangeTask(raftNode, resultFuture, localEndpoint, ADD);
        task.run();
        verify(resultFuture).completeExceptionally(argThat(e ->
                e instanceof NotLeaderException
                        && "localEndpoint is not LEADER of CPGroupId{name='test', seed=1, groupId=1}. Known leader is: N/A".equals(e.getMessage())
        ));

        verify(raftNode, never()).createReplicationTask(any(UpdateRaftGroupMembersCmd.class), eq(resultFuture));
    }

    @Test
    public void testInvalidGroupMemberCommitIndex() {
        when(raftNode.getStatus()).thenReturn(ACTIVE);
        when(raftState.role()).thenReturn(LEADER);
        when(raftState.committedGroupMembers()).thenReturn(new RaftGroupMembers(1, Collections.emptyList(), null));
        MembershipChangeTask task = new MembershipChangeTask(raftNode, resultFuture, localEndpoint, ADD, 2L);
        task.run();
        verify(resultFuture).completeExceptionally(argThat(e ->
                e instanceof MismatchingGroupMembersCommitIndexException
                        && "commit index: 1 members: []".equals(e.getMessage())
        ));
        verify(raftNode, never()).createReplicationTask(any(UpdateRaftGroupMembersCmd.class), eq(resultFuture));
    }

    @Test
    public void testAddMemberReplicates() {
        when(raftNode.getStatus()).thenReturn(ACTIVE);
        when(raftState.role()).thenReturn(LEADER);
        when(raftState.members()).thenReturn(Collections.emptyList());

        ReplicateTask replicateTaskMock = mock(ReplicateTask.class);
        when(raftNode.createReplicationTask(any(), any())).thenReturn(replicateTaskMock);

        MembershipChangeTask task = new MembershipChangeTask(raftNode, resultFuture, localEndpoint, ADD);
        task.run();

        verify(raftNode).createReplicationTask(any(UpdateRaftGroupMembersCmd.class), eq(resultFuture));
        verify(replicateTaskMock).run();
    }

    @Test
    public void testRemoveMemberReplicates() {
        when(raftNode.getStatus()).thenReturn(ACTIVE);
        when(raftState.role()).thenReturn(LEADER);
        when(raftState.members()).thenReturn(Collections.emptyList());

        ReplicateTask replicateTaskMock = mock(ReplicateTask.class);
        when(raftNode.createReplicationTask(any(), any())).thenReturn(replicateTaskMock);
        when(raftState.members()).thenReturn(Collections.singletonList(localEndpoint));

        MembershipChangeTask task = new MembershipChangeTask(raftNode, resultFuture, localEndpoint, REMOVE);
        task.run();

        verify(raftNode).createReplicationTask(any(UpdateRaftGroupMembersCmd.class), eq(resultFuture));
        verify(replicateTaskMock).run();
    }

    @Test
    public void testRemoveMemberFailsWhenMemberDoesntExist() {
        when(raftNode.getStatus()).thenReturn(ACTIVE);
        when(raftState.role()).thenReturn(LEADER);
        when(raftState.members()).thenReturn(Collections.emptyList());

        MembershipChangeTask task = new MembershipChangeTask(raftNode, resultFuture, localEndpoint, REMOVE);
        task.run();

        verify(resultFuture).completeExceptionally(argThat(e ->
                e instanceof MemberDoesNotExistException
                        && ("Member does not exist: " + localEndpoint)
                                .equals(e.getMessage())
        ));
        verify(raftNode, never()).createReplicationTask(any(UpdateRaftGroupMembersCmd.class), eq(resultFuture));
    }
}
