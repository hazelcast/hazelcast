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

import com.hazelcast.sql.impl.exec.AbstractExec;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.mailbox.SendBatch;
import com.hazelcast.sql.impl.mailbox.StripedInbox;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.sort.SortKey;
import com.hazelcast.sql.impl.sort.SortKeyComparator;

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

    /** Input stripes. */
    private final List<Row>[] stripes;

    /** Finished stripes. */
    private final boolean[] stripesDone;

    /** Comparator. */
    private final SortKeyComparator comparator;

    /** Current batch. */
    private RowBatch curBatch;

    /** Whether all sources are available for sorting. */
    private boolean inputsAvailable;

    @SuppressWarnings("unchecked")
    public ReceiveSortMergeExec(int id, StripedInbox inbox, List<Expression> expressions, List<Boolean> ascs) {
        super(id);

        this.inbox = inbox;
        this.expressions = expressions;

        // TODO: If there is only one input edge, then normal ReceiveExec should be used instead, since everything is already
        //  sorted. This should be a part of partition pruning.

        stripes = new List[inbox.getStripeCount()];
        stripesDone = new boolean[inbox.getStripeCount()];

        comparator = new SortKeyComparator(ascs);
    }

    @Override
    public IterationResult advance0() {
        // Try polling inputs.
        pollInputs();

        if (!inputsAvailable) {
            // Some nodes haven't sent their inputs yet, so we cannot proceed.
            // TODO: Something is really wrong here: why we ever return "FETCHED_DONE" if some inputs are not available?
            //  It seems that the whole rewrite of the operator is needed.
            return inbox.closed() ? IterationResult.FETCHED_DONE : IterationResult.WAIT;
        }

        // All inputs available, sort as much as possible.
        prepareBatch();

        if (inbox.closed()) {
            return IterationResult.FETCHED_DONE;
        } else {
            // Apply backpressure.
            inbox.sendFlowControl();

            return IterationResult.FETCHED;
        }
    }

    @Override
    public RowBatch currentBatch0() {
        return curBatch;
    }

    /**
     * Try polling inputs so that at least one batch is available everywhere.
     */
    private void pollInputs() {
        if (inputsAvailable) {
            return;
        }

        boolean res = true;

        for (int i = 0; i < stripes.length; i++) {
            if (stripesDone[i]) {
                continue;
            }

            List<Row> stripeRows = stripes[i];

            // TODO: This implementation is inefficient: we take only one batch from the inbox. What we should do
            //  instead is poll all available batches from queue!
            if (stripeRows == null) {
                while (true) {
                    SendBatch stripeBatch = inbox.poll(i);

                    if (stripeBatch == null) {
                        // No batch available at the moment, wait.
                        res = false;

                        break;
                    } else {
                        if (stripeBatch.isLast()) {
                            stripesDone[i] = true;
                        }

                        List<Row> rows = stripeBatch.getRows();

                        if (!rows.isEmpty()) {
                            stripes[i] = rows;

                            break;
                        }
                    }
                }
            }
        }

        if (res) {
            inputsAvailable = true;
        }
    }

    /**
     * Prepare sorted batch.
     */
    private void prepareBatch() {
        List<Row> rows = new ArrayList<>();

        // Sort entries until inputs are available.
        while (inputsAvailable) {
            SortKey curKey = null;
            int curIdx = -1;

            for (int i = 0; i < stripes.length; i++) {
                List<Row> stripeRows = stripes[i];

                if (stripeRows == null) {
                    assert stripesDone[i];

                    continue;
                }

                assert !stripeRows.isEmpty();

                SortKey stripeKey = prepareSortKey(stripeRows.get(0), i);

                if (curKey == null || comparator.compare(stripeKey, curKey) < 0) {
                    curKey = stripeKey;
                    curIdx = i;
                }
            }

            if (curKey == null) {
                // Avoid infinite loop is all stripes are done.
                break;
            } else {
                List<Row> stripeRows = stripes[curIdx];

                rows.add(stripeRows.remove(0));

                if (stripeRows.isEmpty()) {
                    stripes[curIdx] = null;

                    if (!stripesDone[curIdx]) {
                        inputsAvailable = false;
                    }
                }
            }
        }

        curBatch = new ListRowBatch(rows);
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
            key.add(expression.eval(row));
        }

        return new SortKey(key, stripe);
    }

    @Override
    public boolean canReset() {
        return false;
    }
}
