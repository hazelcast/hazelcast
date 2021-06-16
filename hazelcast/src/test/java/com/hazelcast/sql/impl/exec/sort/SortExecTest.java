/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec.sort;

import com.hazelcast.sql.impl.LoggingQueryOperationHandler;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.UpstreamExec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.exec.fetch.Fetch;
import com.hazelcast.sql.impl.exec.fetch.FetchExec;
import com.hazelcast.sql.impl.exec.io.InboundBatch;
import com.hazelcast.sql.impl.exec.io.ReceiveSortMergeExec;
import com.hazelcast.sql.impl.exec.io.StripedInbox;
import com.hazelcast.sql.impl.exec.io.flowcontrol.simple.SimpleFlowControl;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.SimpleExpressionEvalContext;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.sql.impl.type.QueryDataType.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SortExecTest extends SqlTestSupport {

    @Test
    public void testSortKey() {
        Object[] key = new Object[]{1, null, "foo", Long.MAX_VALUE};
        SortKey sortKey = new SortKey(key, 5);
        assertTrue(Arrays.equals(sortKey.getKey(), key));
        assertEquals(5, sortKey.getIndex());

        SortKey sortKey2 = new SortKey(key, 5);
        assertTrue(sortKey.equals(sortKey2));
        assertTrue(sortKey2.equals(sortKey));
        assertEquals(sortKey.hashCode(), sortKey2.hashCode());

        SortKey sortKey3 = new SortKey(key, 6);
        assertFalse(sortKey3.equals(sortKey));

        SortKey sortKey4 = new SortKey(new Object[]{1, null, "foo1", Long.MAX_VALUE}, 5);
        assertFalse(sortKey4.equals(sortKey));
    }

    @Test
    public void testSortKeyComparatorAscending() {
        boolean[] ascs = new boolean[]{true};
        Comparator<SortKey> comparator = new SortKeyComparator(ascs);

        SortKey sortKey1 = new SortKey(new Object[]{1}, 5);
        SortKey sortKey2 = new SortKey(new Object[]{3}, 5);
        int cmp = comparator.compare(sortKey1, sortKey2);
        assertTrue(cmp < 0);
        cmp = comparator.compare(sortKey2, sortKey1);
        assertTrue(cmp > 0);
        cmp = comparator.compare(sortKey1, sortKey1);
        assertEquals(0, cmp);

        SortKey sortKey3 = new SortKey(new Object[]{3}, 6);
        cmp = comparator.compare(sortKey2, sortKey3);
        assertTrue(cmp < 0);

        SortKey sortKey4 = new SortKey(new Object[]{null}, 7);
        cmp = comparator.compare(sortKey3, sortKey4);
        assertTrue(cmp > 0);
    }

    @Test
    public void testSortKeyComparatorDescending() {
        boolean[] ascs = new boolean[]{false};
        Comparator<SortKey> comparator = new SortKeyComparator(ascs);

        SortKey sortKey1 = new SortKey(new Object[]{1}, 5);
        SortKey sortKey2 = new SortKey(new Object[]{3}, 5);
        int cmp = comparator.compare(sortKey1, sortKey2);
        assertTrue(cmp > 0);
        cmp = comparator.compare(sortKey2, sortKey1);
        assertTrue(cmp < 0);
        cmp = comparator.compare(sortKey1, sortKey1);
        assertEquals(0, cmp);

        SortKey sortKey3 = new SortKey(new Object[]{3}, 6);
        cmp = comparator.compare(sortKey2, sortKey3);
        assertTrue(cmp < 0);

        SortKey sortKey4 = new SortKey(new Object[]{null}, 7);
        cmp = comparator.compare(sortKey3, sortKey4);
        assertTrue(cmp < 0);
    }

    @Test
    public void testSortKeyComparatorComposite() {
        boolean[] ascs = new boolean[]{true, true};
        Comparator<SortKey> comparator = new SortKeyComparator(ascs);

        SortKey sortKey1 = new SortKey(new Object[]{1, 1}, 5);
        SortKey sortKey2 = new SortKey(new Object[]{3, 1}, 5);
        int cmp = comparator.compare(sortKey1, sortKey2);
        assertTrue(cmp < 0);
        cmp = comparator.compare(sortKey2, sortKey1);
        assertTrue(cmp > 0);
        cmp = comparator.compare(sortKey1, sortKey1);
        assertEquals(0, cmp);

        SortKey sortKey3 = new SortKey(new Object[]{1, 3}, 5);
        SortKey sortKey4 = new SortKey(new Object[]{1, 5}, 5);
        cmp = comparator.compare(sortKey3, sortKey4);
        assertTrue(cmp < 0);

        SortKey sortKey5 = new SortKey(new Object[]{null, 5}, 5);
        cmp = comparator.compare(sortKey5, sortKey3);
        assertTrue(cmp < 0);

        SortKey sortKey6 = new SortKey(new Object[]{2, null}, 5);
        cmp = comparator.compare(sortKey3, sortKey6);
        assertTrue(cmp < 0);
    }

    @Test
    public void testStripedInbox() {
        UUID localMemberId = UUID.randomUUID();
        List<UUID> senderMemberIds = Arrays.asList(localMemberId, UUID.randomUUID(), UUID.randomUUID());
        QueryId queryId = QueryId.create(UUID.randomUUID());
        LoggingQueryOperationHandler operationHandler = new LoggingQueryOperationHandler();

        StripedInbox inbox = new StripedInbox(
            operationHandler,
            queryId,
            1,
            true,
            1000,
            localMemberId,
            senderMemberIds,
            new SimpleFlowControl(1_000L, 0.5d)
        );
        inbox.setup();

        assertEquals(3, inbox.getStripeCount());

        inbox.onBatch(new InboundBatch(createMonotonicBatch(0, 10), 0, false, senderMemberIds.get(0)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(0, 12), 0, false, senderMemberIds.get(1)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(0, 14), 0, false, senderMemberIds.get(2)), 100L);

        InboundBatch pollBatch = inbox.poll(0);
        assertNotNull(pollBatch);
        assertFalse(pollBatch.isLast());
        assertEquals(localMemberId, pollBatch.getSenderId());
        assertEquals(10, pollBatch.getBatch().getRowCount());

        pollBatch = inbox.poll(1);
        assertNotNull(pollBatch);
        assertFalse(pollBatch.isLast());
        assertEquals(senderMemberIds.get(1), pollBatch.getSenderId());
        assertEquals(12, pollBatch.getBatch().getRowCount());

        pollBatch = inbox.poll(2);
        assertNotNull(pollBatch);
        assertFalse(pollBatch.isLast());
        assertEquals(senderMemberIds.get(2), pollBatch.getSenderId());
        assertEquals(14, pollBatch.getBatch().getRowCount());

        inbox.onBatch(new InboundBatch(createMonotonicBatch(100, 12), 1, true, senderMemberIds.get(1)), 100L);
        pollBatch = inbox.poll(1);
        assertNotNull(pollBatch);
        assertTrue(pollBatch.isLast());
        assertEquals(senderMemberIds.get(1), pollBatch.getSenderId());
        assertEquals(12, pollBatch.getBatch().getRowCount());

        assertNull(inbox.poll(0));
        assertNull(inbox.poll(2));
    }

    @Test
    public void testMergeSortSources() {
        UUID localMemberId = UUID.randomUUID();
        List<UUID> senderMemberIds = Arrays.asList(localMemberId, UUID.randomUUID(), UUID.randomUUID());
        QueryId queryId = QueryId.create(UUID.randomUUID());
        LoggingQueryOperationHandler operationHandler = new LoggingQueryOperationHandler();

        StripedInbox inbox = new StripedInbox(
            operationHandler,
            queryId,
            1,
            true,
            1000,
            localMemberId,
            senderMemberIds,
            new SimpleFlowControl(1_000L, 0.5d)
        );
        inbox.setup();

        ReceiveSortMergeExec receiveSortMergeExec =
            new ReceiveSortMergeExec(
                1,
                inbox,
                new int[]{0},
                new boolean[]{true},
                null,
                null
            );

        MergeSortSource[] sources = receiveSortMergeExec.getMergeSort().getSources();

        assertEquals(3, sources.length);
        assertFalse(sources[0].advance());
        assertFalse(sources[1].advance());
        assertFalse(sources[2].advance());

        assertFalse(sources[0].isDone());
        assertFalse(sources[1].isDone());
        assertFalse(sources[2].isDone());

        assertNull(sources[0].peekKey());
        assertNull(sources[1].peekKey());
        assertNull(sources[2].peekKey());

        assertNull(sources[0].peekRow());
        assertNull(sources[1].peekRow());
        assertNull(sources[2].peekRow());

        inbox.onBatch(new InboundBatch(createMonotonicBatch(0, 10), 0, false, senderMemberIds.get(0)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(100, 10), 0, false, senderMemberIds.get(1)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(200, 10), 0, false, senderMemberIds.get(2)), 100L);

        for (int i = 0; i < 10; ++i) {
            assertTrue(sources[0].advance());
            assertTrue(sources[1].advance());
            assertTrue(sources[2].advance());

            assertFalse(sources[0].isDone());
            assertFalse(sources[1].isDone());
            assertFalse(sources[2].isDone());

            SortKey sortKey0 = sources[0].peekKey();
            SortKey sortKey1 = sources[1].peekKey();
            SortKey sortKey2 = sources[2].peekKey();
            assertEquals(1, sortKey0.getKey().length);
            assertEquals(1, sortKey1.getKey().length);
            assertEquals(1, sortKey2.getKey().length);

            assertEquals(i, sortKey0.getKey()[0]);
            assertEquals(100 + i, sortKey1.getKey()[0]);
            assertEquals(200 + i, sortKey2.getKey()[0]);
        }

        assertFalse(sources[0].advance());
        assertFalse(sources[1].advance());
        assertFalse(sources[2].advance());

        assertFalse(sources[0].isDone());
        assertFalse(sources[1].isDone());
        assertFalse(sources[2].isDone());

        inbox.onBatch(new InboundBatch(createMonotonicBatch(10, 10), 1, true, senderMemberIds.get(0)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(110, 10), 1, true, senderMemberIds.get(1)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(210, 10), 1, true, senderMemberIds.get(2)), 100L);

        for (int i = 0; i < 10; ++i) {
            assertTrue(sources[0].advance());
            assertTrue(sources[1].advance());
            assertTrue(sources[2].advance());

            if (i == 9) {
                assertTrue(sources[0].isDone());
                assertTrue(sources[1].isDone());
                assertTrue(sources[2].isDone());
            } else {
                assertFalse(sources[0].isDone());
                assertFalse(sources[1].isDone());
                assertFalse(sources[2].isDone());
            }

            SortKey sortKey0 = sources[0].peekKey();
            SortKey sortKey1 = sources[1].peekKey();
            SortKey sortKey2 = sources[2].peekKey();
            assertEquals(1, sortKey0.getKey().length);
            assertEquals(1, sortKey1.getKey().length);
            assertEquals(1, sortKey2.getKey().length);

            assertEquals(10 + i, sortKey0.getKey()[0]);
            assertEquals(110 + i, sortKey1.getKey()[0]);
            assertEquals(210 + i, sortKey2.getKey()[0]);
        }

        assertFalse(sources[0].advance());
        assertFalse(sources[1].advance());
        assertFalse(sources[2].advance());
    }

    @Test
    public void testMergeSort() {
        UUID localMemberId = UUID.randomUUID();
        List<UUID> senderMemberIds = Arrays.asList(localMemberId, UUID.randomUUID(), UUID.randomUUID());
        QueryId queryId = QueryId.create(UUID.randomUUID());
        LoggingQueryOperationHandler operationHandler = new LoggingQueryOperationHandler();

        StripedInbox inbox = new StripedInbox(
            operationHandler,
            queryId,
            1,
            true,
            1000,
            localMemberId,
            senderMemberIds,
            new SimpleFlowControl(1_000L, 0.5d)
        );
        inbox.setup();

        ReceiveSortMergeExec receiveSortMergeExec =
            new ReceiveSortMergeExec(
                1,
                inbox,
                new int[]{0},
                new boolean[]{true},
                null,
                null
            );
        receiveSortMergeExec.setup(emptyFragmentContext());

        MergeSort mergeSort = receiveSortMergeExec.getMergeSort();

        assertEquals(3, mergeSort.getSources().length);

        assertFalse(mergeSort.isDone());
        List<Row> batch = mergeSort.nextBatch();
        assertNull(batch);

        inbox.onBatch(new InboundBatch(createMonotonicBatch(0, 10), 0, false, senderMemberIds.get(0)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(20, 10), 0, false, senderMemberIds.get(1)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(30, 10), 0, false, senderMemberIds.get(2)), 100L);

        assertFalse(mergeSort.isDone());
        batch = mergeSort.nextBatch();
        assertEquals(10, batch.size());
        assertBatch(batch, 0, 9, true);
        assertFalse(mergeSort.isDone());
        assertNull(mergeSort.nextBatch());

        inbox.onBatch(new InboundBatch(createMonotonicBatch(20, 10), 1, true, senderMemberIds.get(0)), 100L);

        batch = mergeSort.nextBatch();
        assertEquals(20, batch.size());
        assertBatch(batch, 20, 29, false);
        assertFalse(mergeSort.isDone());
        assertNull(mergeSort.nextBatch());

        inbox.onBatch(new InboundBatch(createMonotonicBatch(30, 20), 1, true, senderMemberIds.get(1)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(40, 10), 1, false, senderMemberIds.get(2)), 100L);

        batch = mergeSort.nextBatch();
        assertEquals(40, batch.size());
        assertBatch(batch, 30, 49, false);
        assertFalse(mergeSort.isDone());
        assertNull(mergeSort.nextBatch());

        inbox.onBatch(new InboundBatch(createMonotonicBatch(60, 10), 2, true, senderMemberIds.get(2)), 100L);

        batch = mergeSort.nextBatch();
        assertEquals(10, batch.size());
        assertBatch(batch, 60, 69, false);
        assertTrue(mergeSort.isDone());
        assertNull(mergeSort.nextBatch());
    }

    @Test
    public void testReceiveSortMergeExec() {
        UUID localMemberId = UUID.randomUUID();
        List<UUID> senderMemberIds = Arrays.asList(localMemberId, UUID.randomUUID(), UUID.randomUUID());
        QueryId queryId = QueryId.create(UUID.randomUUID());
        LoggingQueryOperationHandler operationHandler = new LoggingQueryOperationHandler();

        StripedInbox inbox = new StripedInbox(
            operationHandler,
            queryId,
            1,
            true,
            1000,
            localMemberId,
            senderMemberIds,
            new SimpleFlowControl(1_000L, 0.5d)
        );
        inbox.setup();

        ReceiveSortMergeExec exec =
            new ReceiveSortMergeExec(
                1,
                inbox,
                new int[]{0},
                new boolean[]{true},
                null,
                null
            );
        exec.setup(emptyFragmentContext());

        assertEquals(IterationResult.WAIT, exec.advance());

        inbox.onBatch(new InboundBatch(createMonotonicBatch(7, 1), 0, false, senderMemberIds.get(0)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(3, 1), 0, false, senderMemberIds.get(1)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(5, 1), 0, false, senderMemberIds.get(2)), 100L);

        assertEquals(IterationResult.FETCHED, exec.advance());
        assertBatch(exec.currentBatch(), 1, 3, 3, true);

        assertEquals(IterationResult.WAIT, exec.advance());

        inbox.onBatch(new InboundBatch(createMonotonicBatch(9, 1), 1, false, senderMemberIds.get(1)), 100L);
        assertEquals(IterationResult.FETCHED, exec.advance());
        assertBatch(exec.currentBatch(), 1, 5, 5, true);

        assertEquals(IterationResult.WAIT, exec.advance());

        inbox.onBatch(new InboundBatch(createMonotonicBatch(11, 2), 1, false, senderMemberIds.get(2)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(13, 3), 2, false, senderMemberIds.get(1)), 100L);
        assertEquals(IterationResult.FETCHED, exec.advance());
        assertBatch(exec.currentBatch(), 1, 7, 7, true);

        inbox.onBatch(new InboundBatch(createMonotonicBatch(15, 4), 1, true, senderMemberIds.get(0)), 100L);
        assertEquals(IterationResult.FETCHED, exec.advance());
        assertBatch(exec.currentBatch(), 3, 9, 12, true);

        inbox.onBatch(new InboundBatch(createMonotonicBatch(0, 0), 2, true, senderMemberIds.get(2)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(21, 7), 3, true, senderMemberIds.get(1)), 100L);
        assertEquals(IterationResult.FETCHED_DONE, exec.advance());
        assertBatch(exec.currentBatch(), 14, 13, 27, false);
    }

    @Test
    public void testLimitOffset() {
        Fetch fetchProcessor = newFetch(2L, 5L);

        RowBatch batch = fetchProcessor.apply(createMonotonicBatch(0, 10));
        assertBatch(batch, 5, 2, 6, true);

        fetchProcessor = newFetch(2L, 5L);
        batch = fetchProcessor.apply(createMonotonicBatch(0, 0));
        assertBatch(batch, 0, 0, 0, true);

        fetchProcessor = newFetch(2L, 5L);
        batch = fetchProcessor.apply(createMonotonicBatch(0, 1));
        assertBatch(batch, 0, 0, 0, true);

        fetchProcessor = newFetch(2L, 5L);
        batch = fetchProcessor.apply(createMonotonicBatch(0, 4));
        assertBatch(batch, 2, 2, 3, true);

        fetchProcessor = newFetch(5L, 10L);
        batch = fetchProcessor.apply(createMonotonicBatch(0, 12));
        assertBatch(batch, 7, 5, 11, true);
    }

    @Test
    public void testOffsetOnly() {
        Fetch fetchProcessor = newFetch(2L, null);
        RowBatch batch = fetchProcessor.apply(createMonotonicBatch(0, 12));
        assertBatch(batch, 10, 2, 11, true);

        fetchProcessor = newFetch(10L, null);
        batch = fetchProcessor.apply(createMonotonicBatch(0, 5));
        assertBatch(batch, 0, 0, 0, true);

        fetchProcessor = newFetch(2L, null);
        batch = fetchProcessor.apply(createMonotonicBatch(0, 20));
        assertBatch(batch, 18, 2, 19, true);
    }

    @Test
    public void testLimitOnly() {
        Fetch fetchProcessor = newFetch(null, 10L);
        RowBatch batch = fetchProcessor.apply(createMonotonicBatch(0, 12));
        assertBatch(batch, 10, 0, 9, true);

        fetchProcessor = newFetch(null, 10L);
        batch = fetchProcessor.apply(createMonotonicBatch(5, 10));
        assertBatch(batch, 10, 5, 14, true);

        fetchProcessor = newFetch(null, 0L);
        batch = fetchProcessor.apply(createMonotonicBatch(5, 10));
        assertBatch(batch, 0, 0, 0, true);
    }

    @Test
    public void testLimitOfsetInvalid() {
        assertThrows(AssertionError.class, () -> newFetch(-1L, 10L));
        assertThrows(AssertionError.class, () -> newFetch(1L, -10L));
        assertThrows(AssertionError.class, () -> newFetch(-1L, -10L));
    }

    @Test
    public void testFetchExec() {
        UpstreamExec upstream = new UpstreamExec(1);

        Expression<?> limit = ConstantExpression.create(10, BIGINT);
        Expression<?> offset = ConstantExpression.create(5, BIGINT);

        FetchExec exec = new FetchExec(2, upstream, limit, offset);
        exec.setup(emptyFragmentContext());

        // Test empty state.
        assertEquals(IterationResult.WAIT, exec.advance());

        upstream.addResult(IterationResult.FETCHED, EmptyRowBatch.INSTANCE);
        assertEquals(IterationResult.WAIT, exec.advance());

        // Consume several batches, still insufficient to produce a result.
        upstream.addResult(IterationResult.FETCHED, createMonotonicBatch(0, 2));
        upstream.addResult(IterationResult.FETCHED, createMonotonicBatch(2, 2));

        assertEquals(IterationResult.WAIT, exec.advance());

        // One more batch, finally producing some rows.
        upstream.addResult(IterationResult.FETCHED, createMonotonicBatch(4, 5));
        assertEquals(IterationResult.FETCHED, exec.advance());
        assertBatch(exec.currentBatch(), 4, 5, 8, true);

        // One more batch
        upstream.addResult(IterationResult.FETCHED, createMonotonicBatch(9, 2));
        assertEquals(IterationResult.FETCHED, exec.advance());
        assertBatch(exec.currentBatch(), 2, 9, 10, true);

        // Final batch to finalize the result
        upstream.addResult(IterationResult.FETCHED, createMonotonicBatch(11, 20));
        assertEquals(IterationResult.FETCHED_DONE, exec.advance());
        assertBatch(exec.currentBatch(), 4, 11, 14, true);
    }

    @Test
    public void testFetchExecInvalid() {
        UpstreamExec upstream = new UpstreamExec(1);

        Expression<?> limit = ConstantExpression.create(null, BIGINT);
        Expression<?> offset = ConstantExpression.create(5, BIGINT);

        FetchExec exec = new FetchExec(2, upstream, limit, offset);
        assertThrows(AssertionError.class, () -> exec.setup(emptyFragmentContext()));

        limit = ConstantExpression.create(5, BIGINT);
        offset = ConstantExpression.create(null, BIGINT);

        FetchExec exec2 = new FetchExec(2, upstream, limit, offset);
        assertThrows(AssertionError.class, () -> exec2.setup(emptyFragmentContext()));

        limit = ConstantExpression.create(5, BIGINT);
        offset = ConstantExpression.create("foo", VARCHAR);

        FetchExec exec3 = new FetchExec(2, upstream, limit, offset);
        assertThrows(AssertionError.class, () -> exec3.setup(emptyFragmentContext()));
    }

    @Test
    public void testReceiveSortMergeExecWithFetchAndOffset() {
        UUID localMemberId = UUID.randomUUID();
        List<UUID> senderMemberIds = Arrays.asList(localMemberId, UUID.randomUUID(), UUID.randomUUID());
        QueryId queryId = QueryId.create(UUID.randomUUID());
        LoggingQueryOperationHandler operationHandler = new LoggingQueryOperationHandler();

        StripedInbox inbox = new StripedInbox(
            operationHandler,
            queryId,
            1,
            true,
            1000,
            localMemberId,
            senderMemberIds,
            new SimpleFlowControl(1_000L, 0.5d)
        );
        inbox.setup();

        Expression<?> fetch = ConstantExpression.create(10, BIGINT);
        Expression<?> offset = ConstantExpression.create(2, BIGINT);
        ReceiveSortMergeExec exec =
            new ReceiveSortMergeExec(
                1,
                inbox,
                new int[]{0},
                new boolean[]{true},
                fetch,
                offset
            );
        exec.setup(emptyFragmentContext());

        assertEquals(IterationResult.WAIT, exec.advance());

        // Consume several batches, still insufficient to produce a result.
        inbox.onBatch(new InboundBatch(createMonotonicBatch(7, 2), 0, false, senderMemberIds.get(0)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(3, 2), 0, false, senderMemberIds.get(1)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(5, 2), 0, false, senderMemberIds.get(2)), 100L);

        assertEquals(IterationResult.WAIT, exec.advance());

        // Consume more batches
        inbox.onBatch(new InboundBatch(createMonotonicBatch(13, 2), 1, false, senderMemberIds.get(0)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(11, 2), 1, false, senderMemberIds.get(1)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(9, 2), 1, false, senderMemberIds.get(2)), 100L);

        assertEquals(IterationResult.FETCHED, exec.advance());
        assertBatch(exec.currentBatch(), 6, 5, 10, true);

        // Consume more batches
        inbox.onBatch(new InboundBatch(createMonotonicBatch(15, 2), 2, false, senderMemberIds.get(0)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(19, 2), 2, false, senderMemberIds.get(1)), 100L);
        inbox.onBatch(new InboundBatch(createMonotonicBatch(17, 2), 2, false, senderMemberIds.get(2)), 100L);

        assertEquals(IterationResult.FETCHED_DONE, exec.advance());
        assertBatch(exec.currentBatch(), 4, 11, 14, true);
    }

    private Fetch newFetch(Long offset, Long limit) {
        Expression<?> limitExpr = limit == null ? null : ConstantExpression.create(limit, BIGINT);
        Expression<?> offsetExpr = offset == null ? null : ConstantExpression.create(offset, BIGINT);
        Fetch fetchProcessor = new Fetch(limitExpr, offsetExpr);

        ExpressionEvalContext evalContext = SimpleExpressionEvalContext.create();

        fetchProcessor.setup(evalContext);
        return fetchProcessor;
    }

    private void assertBatch(List<Row> batch, int low, int high, boolean less) {
        int actualLow = batch.get(0).get(0);
        int actualHigh = batch.get(batch.size() - 1).get(0);
        assertEquals(low, actualLow);
        assertEquals(high, actualHigh);

        Integer prev = null;
        for (int i = 0; i < batch.size(); ++i) {
            if (prev == null) {
                assertEquals(low, actualLow);
                prev = batch.get(i).get(0);
            } else {
                int actual = batch.get(i).get(0);
                int cmp = Integer.compare(prev, actual);

                if (less) {
                    assertTrue(cmp < 0);
                } else {
                    assertTrue(cmp <= 0);
                }
                prev = actual;
            }
        }
    }

    private void assertBatch(RowBatch batch, int expectedCount, int low, int high, boolean less) {
        assertEquals(expectedCount, batch.getRowCount());

        if (batch.getRowCount() == 0) {
            return;
        }
        int actualLow = batch.getRow(0).get(0);
        int actualHigh = batch.getRow(batch.getRowCount() - 1).get(0);
        assertEquals(low, actualLow);
        assertEquals(high, actualHigh);

        Integer prev = null;
        for (int i = 0; i < batch.getRowCount(); ++i) {
            if (prev == null) {
                assertEquals(low, actualLow);
                prev = batch.getRow(i).get(0);
            } else {
                int actual = batch.getRow(i).get(0);
                int cmp = Integer.compare(prev, actual);

                if (less) {
                    assertTrue(cmp < 0);
                } else {
                    assertTrue(cmp <= 0);
                }
                prev = actual;
            }
        }
    }


}
