/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.sort.SortKey;
import com.hazelcast.sql.impl.sort.SortKeyComparator;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Executor which sorts rows from the upstream operator.
 */
public class SortExec extends AbstractUpstreamAwareExec {
    /** Expressions. */
    private final List<Expression> expressions;

    /** Map with sorted result. */
    private final TreeMap<SortKey, Row> map;

    /** Resulting batch. */
    // TODO TODO: Avoid the second data structure. How?
    private RowBatch res;

    /** Index for unique elements. */
    private long idx;

    // TODO TODO: Merge exprs and ascs to a single class, add null-first/last.
    public SortExec(Exec upstream, List<Expression> expressions, List<Boolean> ascs) {
        super(upstream);

        this.expressions = expressions;

        map = new TreeMap<>(new SortKeyComparator(ascs));
    }

    @Override
    public IterationResult advance() {
        while (!upstreamDone) {
            switch (advanceUpstream()) {
                case FETCHED_DONE:
                case FETCHED:
                    consumeBatch(upstreamCurrentBatch);

                    continue;

                case WAIT:
                    return IterationResult.WAIT;

                default:
                    throw new IllegalStateException("Should not reach this.");
            }
        }

        // Blocking stage -> always end up in FETCHED_DONE state.
        finalizeResult();

        return IterationResult.FETCHED_DONE;
    }

    @Override
    public RowBatch currentBatch() {
        return res != null ? res : EmptyRowBatch.INSTANCE;
    }

    /**
     * Consume a batch.
     *
     * @param batch Batch.
     */
    private void consumeBatch(RowBatch batch) {
        for (int i = 0; i < batch.getRowCount(); i++)
            consumeRow(batch.getRow(i));
    }

    /**
     * Consume a single row.
     *
     * @param row Row.
     */
    private void consumeRow(Row row) {
        List<Object> key = new ArrayList<>(expressions.size());

        for (Expression expression : expressions)
            key.add(expression.eval(ctx, row));

        map.put(new SortKey(key, idx++), row);
    }

    /**
     * Finalize the result.
     */
    private void finalizeResult() {
        if (res != null)
            return;

        List<Row> resList = new ArrayList<>(map.size());

        resList.addAll(map.values());

        res = new ListRowBatch(resList);
    }
}
