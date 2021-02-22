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

package com.hazelcast.sql.impl.exec.io;

import com.hazelcast.sql.impl.exec.AbstractExec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.exec.sort.MergeSort;
import com.hazelcast.sql.impl.exec.sort.MergeSortSource;
import com.hazelcast.sql.impl.exec.sort.SortKey;
import com.hazelcast.sql.impl.exec.sort.SortKeyComparator;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;

/**
 * Executor which receives entries from multiple sources and merges them into a single sorted stream.
 */
@SuppressWarnings("rawtypes")
@SuppressFBWarnings("EI_EXPOSE_REP2")
public class ReceiveSortMergeExec extends AbstractExec {
    /**
     * AbstractInbox to consume results from.
     */
    private final StripedInbox inbox;

    /**
     * Indexes of columns to be used for sorting.
     */
    private final int[] columnIndexes;

    /**
     * Sorter.
     */
    private final MergeSort sorter;

    /**
     * Current batch.
     */
    private RowBatch curBatch;

    public ReceiveSortMergeExec(
        int id,
        StripedInbox inbox,
        int[] columnIndexes,
        boolean[] ascs,
        Expression fetch,
        Expression offset
    ) {
        super(id);

        this.inbox = inbox;
        this.columnIndexes = columnIndexes;

        MergeSortSource[] sources = new MergeSortSource[inbox.getStripeCount()];

        for (int i = 0; i < inbox.getStripeCount(); i++) {
            sources[i] = new Source(i);
        }

        sorter = new MergeSort(sources, new SortKeyComparator(ascs), fetch, offset);
    }

    @Override
    protected void setup0(QueryFragmentContext ctx) {
        inbox.setup();

        sorter.setup(ctx);
    }

    @Override
    public IterationResult advance0() {
        List<Row> rows = sorter.nextBatch();
        boolean done = sorter.isDone();

        if (rows == null || rows.size() == 0) {
            curBatch = EmptyRowBatch.INSTANCE;

            return done ? IterationResult.FETCHED_DONE : IterationResult.WAIT;
        } else {
            RowBatch batch = new ListRowBatch(rows);

            curBatch = batch;

            return done ? IterationResult.FETCHED_DONE : IterationResult.FETCHED;
        }

    }

    @Override
    public RowBatch currentBatch0() {
        return curBatch;
    }

    /**
     * Prepare sort key for the row.
     *
     * @param row    Row.
     * @param stripe Source stripe.
     * @return Key.
     */
    private SortKey prepareSortKey(Row row, int stripe) {
        Object[] key = new Object[columnIndexes.length];

        for (int i = 0; i < columnIndexes.length; ++i) {
            int idx = columnIndexes[i];
            key[i] = row.get(idx);
        }

        return new SortKey(key, stripe);
    }

    private final class Source implements MergeSortSource {
        private static final int INDEX_BEFORE = -1;

        /**
         * Index of stripe.
         */
        private final int index;

        /**
         * Current batch we are iterating over.
         */
        private RowBatch curBatch;

        /**
         * Current position in the batch.
         */
        private int curRowIndex = INDEX_BEFORE;

        /**
         * Current key.
         */
        private SortKey curKey;

        /**
         * Current row.
         */
        private Row curRow;

        /**
         * Whether the last batch was polled.
         */
        private boolean last;

        private Source(int index) {
            this.index = index;
        }

        @Override
        public boolean advance() {
            // Get the next batch if needed.
            while (noBatch()) {
                if (last) {
                    return false;
                }

                InboundBatch batch = inbox.poll(index);

                if (batch == null) {
                    return false;
                }

                last = batch.isLast();

                RowBatch batch0 = batch.getBatch();

                if (batch0.getRowCount() != 0) {
                    curBatch = batch0;
                    curRowIndex = 0;

                    break;
                }
            }

            // At this point we should have a batch with at least one row, so get it.
            assert curRowIndex < curBatch.getRowCount();

            // Get row and key.
            curRow = curBatch.getRow(curRowIndex++);
            curKey = prepareSortKey(curRow, index);

            // Check if batch is over.
            if (curRowIndex == curBatch.getRowCount()) {
                curBatch = null;
                curRowIndex = INDEX_BEFORE;
            }

            return true;
        }

        @Override
        public boolean isDone() {
            return noBatch() && last;
        }

        @Override
        public SortKey peekKey() {
            return curKey;
        }

        @Override
        public Row peekRow() {
            return curRow;
        }

        private boolean noBatch() {
            return curRowIndex == INDEX_BEFORE;
        }
    }

    // For unit testing only
    public MergeSort getMergeSort() {
        return sorter;
    }
}
