/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
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

    @Before
    public void setUp() {
        schemaService = mock(MemberSchemaService.class);
        replicator = spy(new SchemaReplicator(schemaService));
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getMembers()).thenReturn(Collections.emptySet());
        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getClusterService()).thenReturn(clusterService);
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
        CompletableFuture<Void> future = makeSchemaServicePrepareLater();

        Schema schema = createSchema();
        CompletableFuture<Void> future1 = replicator.replicate(schema);
        CompletableFuture<Void> future2 = replicator.replicate(schema);

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

        doReturn(CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        })).when(replicator).sendRequestForPreparation(schema);

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

        doReturn(CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        })).when(replicator).sendRequestForAcknowledgment(schema.getSchemaId());

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

        doReturn(CompletableFuture.supplyAsync(() -> { // throw once
            throw new RuntimeException();
        })).doReturn(CompletableFuture.completedFuture(null)) // then succeed
                .when(replicator).sendRequestForPreparation(schema);

        assertThrows(RuntimeException.class, () -> replicator.replicate(schema).join());
        replicator.replicate(schema).join();

        verify(replicator, times(1)).markSchemaAsPrepared(schema);
        assertEquals(1, replicator.getReplications().size());
        assertEquals(SchemaReplicationStatus.REPLICATED, replicator.getReplicationStatus(schema));
        assertEquals(0, replicator.getInFlightOperations().size());
    }

    private void makeSchemaServicePrepareImmediately() {
        when(schemaService.persistSchemaToHotRestartAsync(any())).thenReturn(CompletableFuture.completedFuture(null));
    }

    private CompletableFuture<Void> makeSchemaServicePrepareLater() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        when(schemaService.persistSchemaToHotRestartAsync(any())).thenReturn(future);
        return future;
    }

    private void makeSchemaServiceFailImmediately() {
        when(schemaService.persistSchemaToHotRestartAsync(any())).thenReturn(CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        }));
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
