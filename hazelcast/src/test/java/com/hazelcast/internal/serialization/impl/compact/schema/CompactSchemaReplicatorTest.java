/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact.schema;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.instance.SimpleMemberImpl;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.internal.util.executor.CachedExecutorServiceDelegate;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactSchemaReplicatorTest extends HazelcastTestSupport {
    private SchemaReplicator replicator;
    private MemberSchemaService schemaService;
    private ClusterService clusterService;
    private ExecutionService executionService;

    @Before
    public void setUp() {
        schemaService = mock(MemberSchemaService.class);
        replicator = spy(new SchemaReplicator(schemaService));
        clusterService = mock(ClusterService.class);
        executionService = mock(ExecutionService.class);
        when(executionService.getExecutor(ExecutionService.ASYNC_EXECUTOR))
                .thenReturn(new CachedExecutorServiceDelegate("test", new ForkJoinPool(3),
                        8, 1000));
        when(clusterService.getMembers()).thenReturn(Collections.emptySet());
        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getClusterService()).thenReturn(clusterService);
        when(nodeEngine.getProperties()).thenReturn(new HazelcastProperties(new Config()));
        when(nodeEngine.getExecutionService()).thenReturn(executionService);
        replicator.init(nodeEngine);
    }

    @Test
    public void testReplicate() {
        makeSchemaServicePrepareImmediately();

        Schema schema = createSchema();
        replicator.replicate(schema).join();

        verify(replicator, times(1)).markSchemaAsPrepared(schema);
        assertEquals(1, replicator.getReplications().size());
        assertEquals(SchemaReplicationStatus.REPLICATED, replicator.getReplicationStatus(schema));
        assertEquals(0, replicator.getInFlightOperations().size());
    }

    @Test
    public void testReplicate_returnsReplicatedMemberUuids() {
        makeSchemaServicePrepareImmediately();

        SimpleMemberImpl member = new SimpleMemberImpl(MemberVersion.UNKNOWN, UUID.randomUUID(), new InetSocketAddress("127.0.0.1", 55555));

        // start with a single member
        when(clusterService.getMembers())
                .thenReturn(Collections.singleton(member));

        doReturn(InternalCompletableFuture.newCompletedFuture(Collections.singleton(member.getUuid())))
                .when(replicator)
                .sendRequestForPreparation(any());
        doReturn(InternalCompletableFuture.newCompletedFuture(Collections.singleton(member.getUuid())))
                .when(replicator)
                .sendRequestForAcknowledgment(anyLong());

        Schema schema = createSchema();
        Collection<UUID> uuids = replicator.replicate(schema).join();

        // the replication should return the member uuid
        assertThat(uuids)
                .containsExactlyInAnyOrder(member.getUuid());

        Set<Member> members = new HashSet<>();
        members.add(member);

        SimpleMemberImpl newMember = new SimpleMemberImpl(MemberVersion.UNKNOWN, UUID.randomUUID(), new InetSocketAddress("127.0.0.1", 55556));
        members.add(newMember);

        // assume that the member list changes
        when(clusterService.getMembers())
                .thenReturn(members);

        // for already replicated schemas, the new member list should be returned
        uuids = replicator.replicate(schema).join();
        assertThat(uuids)
                .containsExactlyInAnyOrder(member.getUuid(), newMember.getUuid());
    }

    @Test
    public void testReplicate_withMultipleInvocationsWithTheSameSchema() {
        makeSchemaServicePrepareImmediately();

        Schema schema = createSchema();
        replicator.replicate(schema).join();
        replicator.replicate(schema).join();
        replicator.replicate(schema).join();

        verify(replicator, times(1)).markSchemaAsPrepared(schema);
        assertEquals(1, replicator.getReplications().size());
        assertEquals(SchemaReplicationStatus.REPLICATED, replicator.getReplicationStatus(schema));
        assertEquals(0, replicator.getInFlightOperations().size());
    }

    @Test
    public void testReplicate_withDifferentSchemas() {
        makeSchemaServicePrepareImmediately();

        Schema schema1 = createSchema();
        Schema schema2 = createSchema();
        replicator.replicate(schema1).join();
        replicator.replicate(schema2).join();

        verify(replicator, times(1)).markSchemaAsPrepared(schema1);
        verify(replicator, times(1)).markSchemaAsPrepared(schema2);
        assertEquals(2, replicator.getReplications().size());
        assertEquals(SchemaReplicationStatus.REPLICATED, replicator.getReplicationStatus(schema1));
        assertEquals(SchemaReplicationStatus.REPLICATED, replicator.getReplicationStatus(schema2));
        assertEquals(0, replicator.getInFlightOperations().size());
    }

    @Test
    public void testReplicate_returnsTheSameFutureForOngoingOperations() {
        InternalCompletableFuture<Void> future = makeSchemaServicePrepareLater();

        Schema schema = createSchema();
        InternalCompletableFuture<Collection<UUID>> future1 = replicator.replicate(schema);
        InternalCompletableFuture<Collection<UUID>> future2 = replicator.replicate(schema);

        assertSame(future1, future2);

        // Make schema service complete local preparation
        future.complete(null);

        future1.join();
        future2.join();

        verify(replicator, times(1)).markSchemaAsPrepared(schema);
        assertEquals(1, replicator.getReplications().size());
        assertEquals(SchemaReplicationStatus.REPLICATED, replicator.getReplicationStatus(schema));
        assertEquals(0, replicator.getInFlightOperations().size());
    }

    @Test
    public void testReplicate_whenLocalPreparationFails() {
        makeSchemaServiceFailImmediately();

        assertThrows(RuntimeException.class, () -> replicator.replicate(createSchema()).join());

        verify(replicator, never()).markSchemaAsPrepared(any());
        assertEquals(0, replicator.getReplications().size());
        assertEquals(0, replicator.getInFlightOperations().size());
    }

    @Test
    public void testReplicate_whenLocalPreparationFailsSynchronously() {
        makeSchemaServiceSynchronouslyFailImmediately();

        assertThrows(RuntimeException.class, () -> replicator.replicate(createSchema()).join());

        verify(replicator, never()).markSchemaAsPrepared(any());
        assertEquals(0, replicator.getReplications().size());
        assertEquals(0, replicator.getInFlightOperations().size());
    }

    @Test
    public void testReplicate_whenPreparationPhaseFails() {
        makeSchemaServicePrepareImmediately();

        Schema schema = createSchema();

        InternalCompletableFuture<Void> future = new InternalCompletableFuture<>();
        future.completeExceptionally(new RuntimeException());
        doReturn(future)
                .when(replicator)
                .sendRequestForPreparation(schema);

        assertThrows(RuntimeException.class, () -> replicator.replicate(schema).join());

        verify(replicator, times(1)).markSchemaAsPrepared(schema);
        assertEquals(1, replicator.getReplications().size());
        assertEquals(SchemaReplicationStatus.PREPARED, replicator.getReplicationStatus(schema));
        assertEquals(0, replicator.getInFlightOperations().size());
    }

    @Test
    public void testReplicate_whenAcknowledgmentPhaseFails() {
        makeSchemaServicePrepareImmediately();

        Schema schema = createSchema();

        InternalCompletableFuture<Void> future = new InternalCompletableFuture<>();
        future.completeExceptionally(new RuntimeException());
        doReturn(future)
                .when(replicator)
                .sendRequestForAcknowledgment(schema.getSchemaId());

        assertThrows(RuntimeException.class, () -> replicator.replicate(schema).join());

        verify(replicator, times(1)).markSchemaAsPrepared(schema);
        assertEquals(1, replicator.getReplications().size());
        assertEquals(SchemaReplicationStatus.PREPARED, replicator.getReplicationStatus(schema));
        assertEquals(0, replicator.getInFlightOperations().size());
    }

    @Test
    public void testReplicate_preparedSchema() {
        makeSchemaServicePrepareImmediately();

        Schema schema = createSchema();

        InternalCompletableFuture<Void> future = new InternalCompletableFuture<>();
        future.completeExceptionally(new RuntimeException());
        doReturn(future) // throw once
                .doReturn(InternalCompletableFuture.newCompletedFuture(null)) // then succeed
                .when(replicator).sendRequestForPreparation(schema);

        assertThrows(RuntimeException.class, () -> replicator.replicate(schema).join());
        replicator.replicate(schema).join();

        verify(replicator, times(1)).markSchemaAsPrepared(schema);
        assertEquals(1, replicator.getReplications().size());
        assertEquals(SchemaReplicationStatus.REPLICATED, replicator.getReplicationStatus(schema));
        assertEquals(0, replicator.getInFlightOperations().size());
    }

    private void makeSchemaServicePrepareImmediately() {
        when(schemaService.persistSchemaToHotRestartAsync(any())).thenReturn(InternalCompletableFuture.newCompletedFuture(null));
    }

    private InternalCompletableFuture<Void> makeSchemaServicePrepareLater() {
        InternalCompletableFuture<Void> future = new InternalCompletableFuture<>();
        when(schemaService.persistSchemaToHotRestartAsync(any())).thenReturn(future);
        return future;
    }

    private void makeSchemaServiceFailImmediately() {
        InternalCompletableFuture<Void> future = new InternalCompletableFuture<>();
        future.completeExceptionally(new RuntimeException());
        when(schemaService.persistSchemaToHotRestartAsync(any())).thenReturn(future);
    }

    private void makeSchemaServiceSynchronouslyFailImmediately() {
        doAnswer(invocation -> {
            throw new RuntimeException();
        }).when(schemaService).persistSchemaToHotRestartAsync(any());
    }

    private Schema createSchema() {
        return new Schema(randomString(), new ArrayList<>());
    }
}
