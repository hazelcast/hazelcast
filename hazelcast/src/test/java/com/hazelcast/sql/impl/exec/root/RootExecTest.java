/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec.root;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.exec.AbstractExec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RootExecTest extends SqlTestSupport {

    private int rowCounter;

    @Test
    public void testAdvance() {
        TestUpstreamExec upstream = new TestUpstreamExec(1);
        TestConsumer consumer = new TestConsumer();

        RootExec exec = new RootExec(2, upstream, consumer, 8);

        // Make sure that the context is propagated.
        QueryFragmentContext context = emptyFragmentContext();

        exec.setup(context);

        assertSame(context, consumer.getContext());

        assertEquals(IterationResult.WAIT, exec.advance());
        assertEquals(0, consumer.getRowCount());

        upstream.addResult(IterationResult.FETCHED, createRows(6));
        upstream.addResult(IterationResult.FETCHED, createRows(6));
        upstream.addResult(IterationResult.FETCHED, createRows(6));
        upstream.addResult(IterationResult.FETCHED_DONE, createRows(8));

        assertEquals(IterationResult.WAIT, exec.advance());
        checkRows(consumer.pollRows(), 8, 0);

        assertEquals(IterationResult.WAIT, exec.advance());
        checkRows(consumer.pollRows(), 8, 8);

        assertEquals(IterationResult.WAIT, exec.advance());
        checkRows(consumer.pollRows(), 8, 16);

        assertFalse(consumer.isLast());
        assertEquals(IterationResult.FETCHED_DONE, exec.advance());
        checkRows(consumer.pollRows(), 2, 24);
        assertTrue(consumer.isLast());
    }

    @Test
    public void testCurrentBatch() {
        RootExec exec = new RootExec(2, new TestUpstreamExec(1), new TestConsumer(), 1000);

        assertThrows(UnsupportedOperationException.class, exec::currentBatch);
    }

    private ListRowBatch createRows(int count) {
        List<Row> rows = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            rows.add(new HeapRow(new Object[] { rowCounter++ }));
        }

        return new ListRowBatch(rows);
    }

    private void checkRows(List<Row> rows, int expectedCount, int expectedStartCounter) {
        assertEquals(expectedCount, rows.size());

        for (int i = 0; i < expectedCount; i++) {
            int value = rows.get(i).get(0);

            assertEquals(expectedStartCounter + i, value);
        }
    }

    private static final class UpstreamResult {
        private final IterationResult result;
        private final RowBatch batch;

        private UpstreamResult(IterationResult result, RowBatch batch) {
            this.result = result;
            this.batch = batch;
        }

        private IterationResult getResult() {
            return result;
        }

        private RowBatch getBatch() {
            return batch;
        }
    }

    private static final class TestUpstreamExec extends AbstractExec {

        private final ArrayDeque<UpstreamResult> results = new ArrayDeque<>();
        private UpstreamResult currentResult;

        private TestUpstreamExec(int id) {
            super(id);
        }

        @Override
        protected IterationResult advance0() {
            currentResult = results.poll();

            if (currentResult == null) {
                return IterationResult.WAIT;
            }

            return currentResult.getResult();
        }

        @Override
        protected RowBatch currentBatch0() {
            return currentResult.getBatch();
        }

        private void addResult(IterationResult result, RowBatch batch) {
            results.add(new UpstreamResult(result, batch));
        }
    }

    private static final class TestConsumer implements RootResultConsumer {

        private QueryFragmentContext context;

        private List<Row> rows;
        private boolean last;

        @Override
        public Iterator<Row> iterator() {
            return null;
        }

        @Override
        public void onError(HazelcastSqlException error) {
            // No-op.
        }

        @Override
        public void setup(QueryFragmentContext context) {
            this.context = context;
        }

        @Override
        public boolean consume(List<Row> rows, boolean last) {
            if (this.rows != null) {
                return false;
            } else {
                this.rows = rows;
                this.last = last;

                return true;
            }
        }

        private QueryFragmentContext getContext() {
            return context;
        }

        private int getRowCount() {
            return rows != null ? rows.size() : 0;
        }

        private List<Row> pollRows() {
            List<Row> rows0 = rows;

            rows = null;

            return rows0 != null ? rows0 : Collections.emptyList();
        }

        private boolean isLast() {
            return last;
        }
    }
}
