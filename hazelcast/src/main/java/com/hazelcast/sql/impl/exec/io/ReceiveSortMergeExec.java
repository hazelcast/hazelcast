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

package com.hazelcast.sql.impl.exec.io;

import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.sql.impl.exec.AbstractExec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.exec.fetch.Fetch;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.exec.sort.MergeSortSource;
import com.hazelcast.sql.impl.exec.sort.MergeSort;
import com.hazelcast.sql.impl.exec.sort.SortKey;
import com.hazelcast.sql.impl.exec.sort.SortKeyComparator;

import java.util.ArrayList;
import java.util.List;

/**
 * Executor which receives entries from multiple sources and merge them into a single sorted stream.
 */
@SuppressWarnings("rawtypes")
public class ReceiveSortMergeExec extends AbstractExec {
    /** AbstractInbox to consume results from. */
    private final StripedInbox inbox;

    /** Expressions. */
    private final List<Expression> expressions;

    /** Sorter. */
    private final MergeSort sorter;

    /** Fetch processor. */
    private final Fetch fetch;

    /** Current batch. */
    private RowBatch curBatch;

    public ReceiveSortMergeExec(
        int id,
        StripedInbox inbox,
        List<Expression> expressions,
        List<Boolean> ascs,
        Expression fetch,
        Expression offset
    ) {
        super(id);

        this.inbox = inbox;
        this.expressions = expressions;

        // TODO: If there is only one input edge, then normal ReceiveExec should be used instead, since everything is already
        //  sorted. This should be a part of partition pruning.

        MergeSortSource[] sources = new MergeSortSource[inbox.getStripeCount()];

        for (int i = 0; i < inbox.getStripeCount(); i++) {
            sources[i] = new Source(i);
        }

        // TODO: Pass limit here if any.
        sorter = new MergeSort(sources, new SortKeyComparator(ascs));

        this.fetch = fetch != null ? new Fetch(fetch, offset) : null;
    }

    @Override
    protected void setup0(QueryFragmentContext ctx) {
        inbox.setup();

        if (fetch != null) {
            fetch.setup(ctx);
        }
    }

    @Override
    public IterationResult advance0() {
        while (true) {
            List<Row> rows = sorter.nextBatch();
            boolean done = sorter.isDone();

            if (rows == null) {
                curBatch = EmptyRowBatch.INSTANCE;

                return done ? IterationResult.FETCHED_DONE : IterationResult.WAIT;
            } else {
                RowBatch batch = new ListRowBatch(rows);

                if (fetch != null) {
                    batch = fetch.apply(batch);
                    done |= fetch.isDone();

                    if (batch.getRowCount() == 0 && !done) {
                        continue;
                    }
                }

                curBatch = batch;

                return done ? IterationResult.FETCHED_DONE : IterationResult.FETCHED;
            }
        }
    }

    @Override
    public RowBatch currentBatch0() {
        return curBatch;
    }

    /**
     * Prepare sort key for the row.
     *
     * @param row Row.
     * @param stripe Source stripe.
     * @return Key.
     */
    private SortKey prepareSortKey(Row row, int stripe) {
        List<Object> key = new ArrayList<>(expressions.size());

        for (Expression<?> expression : expressions) {
            key.add(expression.eval(row, ctx));
        }

        return new SortKey(key, stripe);
    }

    @Override
    public boolean canReset() {
        return false;
    }

    private final class Source implements MergeSortSource {
        private static final int INDEX_BEFORE = -1;

        /** Index of stripe. */
        private final int index;

        /** Current batch we are iterating over. */
        private RowBatch curBatch;

        /** Current position in the batch. */
        private int curRowIndex = INDEX_BEFORE;

        /** Current key. */
        private SortKey curKey;

        /** Current row. */
        private Row curRow;

        /** Whether the last batch was polled. */
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
}
