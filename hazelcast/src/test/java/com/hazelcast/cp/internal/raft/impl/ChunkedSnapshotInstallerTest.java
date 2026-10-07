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

package com.hazelcast.cp.internal.raft.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.cp.internal.RaftEndpointImpl;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotRequest;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotResponse;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotChunk;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.cp.internal.datastructures.snapshot.ChunkUtil.extractChunksFrom;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ChunkedSnapshotInstallerTest {

    private ChunkedSnapshotInstaller installer;
    private RaftNodeImpl raftNode;
    private RaftState raftState;
    private RaftStateStore stateStore;
    private RaftIntegration raftIntegration;
    private RaftEndpoint localEndpoint;
    private RaftEndpoint leader;

    @Before
    public void setup() {
        raftNode = mock(RaftNodeImpl.class, RETURNS_MOCKS);
        ILogger logger = mock(ILogger.class);
        stateStore = mock(RaftStateStore.class);
        raftState = mock(RaftState.class);
        raftIntegration = mock(RaftIntegration.class);
        localEndpoint = new RaftEndpointImpl(UUID.randomUUID());
        leader = new RaftEndpointImpl(UUID.randomUUID());

        when(raftNode.state()).thenReturn(raftState);
        when(raftNode.getLogger(any())).thenReturn(logger);
        when(raftNode.getRaftIntegration()).thenReturn(raftIntegration);
        when(raftState.stateStore()).thenReturn(stateStore);
        when(raftState.commitIndex()).thenReturn(10L);
        when(raftState.localEndpoint()).thenReturn(localEndpoint);
        when(raftState.term()).thenReturn(1);
        when(raftNode.getLocalMember()).thenReturn(localEndpoint);
        when(raftNode.getLeader()).thenReturn(leader);

        installer = new ChunkedSnapshotInstaller(raftNode);
    }

    private static SnapshotChunk newSnapshotChunk(long index, int chunkCount, int chunkNumber, int term) {
        SnapshotChunk snapshotChunk = new SnapshotChunk();
        snapshotChunk.setGroupMembers(singletonList(new RaftEndpointImpl(UUID.randomUUID())));
        return (SnapshotChunk) snapshotChunk
                .setChunkCount(chunkCount)
                .setChunkNumber(chunkNumber)
                .setSnapshotTerm(term)
                .setIndex(index)
                .setOperation(new Object());
    }

    private static SnapshotChunk newMetadataChunk(long index, int chunkCount, int chunkNumber, int term) {
        return (SnapshotChunk) newSnapshotChunk(index, chunkCount, chunkNumber, term).setOperation(null);
    }

    private InstallSnapshotRequest newRequest(SnapshotChunk chunk) {
        return new InstallSnapshotRequest(leader, 1, chunk, 7, 123L);
    }

    @Test
    public void cleans_up_partial_transfer_when_newer_snapshot_metadata_is_received() throws IOException {
        SnapshotChunk oldChunk = newSnapshotChunk(12L, 2, 0, 1);
        installer.processChunk(newRequest(oldChunk));

        SnapshotChunk newerMetadata = newMetadataChunk(13L, 2, 0, 1);
        installer.processChunk(newRequest(newerMetadata));

        verify(stateStore).deleteSnapshotChunks(12L);
        assertEquals(0, installer.getReceivedChunks().size());
    }

    @Test
    public void ignores_stale_snapshot_chunk() throws IOException {
        when(raftState.commitIndex()).thenReturn(15L);
        SnapshotChunk chunk = newMetadataChunk(12L, 1, 0, 1);

        installer.processChunk(newRequest(chunk));

        assertEquals(0, installer.getReceivedChunks().size());
        verify(stateStore, never()).persistSnapshotChunk(any());
    }

    @Test
    public void persists_duplicate_data_chunk_only_once() throws IOException {
        SnapshotChunk chunk = newSnapshotChunk(13L, 2, 0, 1);
        InstallSnapshotRequest request = newRequest(chunk);

        installer.processChunk(request);
        installer.processChunk(request);

        assertEquals(1, installer.getReceivedChunks().size());
        verify(stateStore).persistSnapshotChunk(chunk);
    }

    @Test
    public void installs_out_of_order_chunks_in_chunk_number_order() {
        SnapshotChunk chunk2 = newSnapshotChunk(13L, 3, 2, 1);
        SnapshotChunk chunk0 = newSnapshotChunk(13L, 3, 0, 1);
        SnapshotChunk chunk1 = newSnapshotChunk(13L, 3, 1, 1);

        installer.processChunk(newRequest(chunk2));
        installer.processChunk(newRequest(chunk0));
        installer.processChunk(newRequest(chunk1));

        ArgumentCaptor<SnapshotEntry> snapshotCaptor = ArgumentCaptor.forClass(SnapshotEntry.class);
        verify(raftNode).installSnapshot(snapshotCaptor.capture());
        List<SnapshotChunk> installedChunks = extractChunksFrom(snapshotCaptor.getValue());
        assertEquals(3, installedChunks.size());
        assertSame(chunk0, installedChunks.get(0));
        assertSame(chunk1, installedChunks.get(1));
        assertSame(chunk2, installedChunks.get(2));
    }

    @Test
    public void flushes_snapshot_before_resetting_services_and_installing_snapshot() throws IOException {
        SnapshotChunk chunk = newSnapshotChunk(13L, 1, 0, 1);

        installer.processChunk(newRequest(chunk));

        InOrder order = inOrder(stateStore, raftIntegration, raftNode);
        order.verify(stateStore).persistSnapshotChunk(chunk);
        order.verify(stateStore).flushLogs();
        order.verify(raftIntegration).resetForChunkedSnapshotRestore();
        order.verify(raftNode).installSnapshot(any(SnapshotEntry.class));
        order.verify(raftNode).send(any(AppendSuccessResponse.class), eq(leader));
    }

    @Test
    public void does_not_reset_services_when_completed_snapshot_cannot_be_flushed() throws IOException {
        doThrow(new IOException("expected")).when(stateStore).flushLogs();
        SnapshotChunk chunk = newSnapshotChunk(13L, 1, 0, 1);
        InstallSnapshotRequest request = newRequest(chunk);

        Assert.assertThrows(HazelcastException.class, () -> installer.processChunk(request));

        verify(raftIntegration, never()).resetForChunkedSnapshotRestore();
        verify(raftNode, never()).installSnapshot(any());
        verify(raftNode, never()).send(any(AppendSuccessResponse.class), any(RaftEndpoint.class));
    }

    @Test
    public void retries_chunk_after_persistence_failure() throws IOException {
        SnapshotChunk chunk = newSnapshotChunk(13L, 1, 0, 1);
        InstallSnapshotRequest request = newRequest(chunk);
        doThrow(new IOException("expected")).doNothing().when(stateStore).persistSnapshotChunk(chunk);

        Assert.assertThrows(HazelcastException.class, () -> installer.processChunk(request));

        assertEquals(0, installer.getReceivedChunks().size());
        verify(raftIntegration, never()).resetForChunkedSnapshotRestore();
        verify(raftNode, never()).installSnapshot(any());
        verify(raftNode, never()).send(any(AppendSuccessResponse.class), any(RaftEndpoint.class));

        installer.processChunk(request);

        verify(stateStore, times(2)).persistSnapshotChunk(chunk);
        verify(raftIntegration).resetForChunkedSnapshotRestore();
        verify(raftNode).installSnapshot(any(SnapshotEntry.class));
    }

    @Test
    public void rejects_chunk_with_inconsistent_snapshot_term() throws IOException {
        SnapshotChunk firstChunk = newSnapshotChunk(13L, 2, 0, 1);
        SnapshotChunk inconsistentChunk = newSnapshotChunk(13L, 2, 1, 2);
        installer.processChunk(newRequest(firstChunk));

        Assert.assertThrows(IllegalStateException.class,
                () -> installer.processChunk(newRequest(inconsistentChunk)));

        assertEquals(1, installer.getReceivedChunks().size());
        verify(stateStore).persistSnapshotChunk(firstChunk);
        verify(stateStore, never()).persistSnapshotChunk(inconsistentChunk);
        verify(raftNode, never()).installSnapshot(any());
    }

    @Test
    public void requests_lowest_missing_chunk_with_transfer_metadata() {
        SnapshotChunk chunk = newSnapshotChunk(13L, 3, 1, 1);

        installer.processChunk(newRequest(chunk));

        ArgumentCaptor<InstallSnapshotResponse> responseCaptor =
                ArgumentCaptor.forClass(InstallSnapshotResponse.class);
        verify(raftIntegration).send(responseCaptor.capture(), eq(leader));
        InstallSnapshotResponse response = responseCaptor.getValue();
        assertSame(localEndpoint, response.follower());
        assertEquals(1, response.term());
        assertEquals(7, response.queryRound());
        assertEquals(123L, response.flowControlSequenceNumber());
        assertEquals(13L, response.snapshotIndex());
        assertEquals(0, response.requestedChunkNumber());
    }
}
