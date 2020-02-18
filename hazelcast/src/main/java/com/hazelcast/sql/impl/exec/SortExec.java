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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.sql.impl.expression.Expression;
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
    private RowBatch res;

    /** Index for unique elements. */
    private long idx;

    public SortExec(int id, Exec upstream, List<Expression> expressions, List<Boolean> ascs) {
        super(id, upstream);

        this.expressions = expressions;

        map = new TreeMap<>(new SortKeyComparator(ascs));
    }

    @Override
    public IterationResult advance0() {
        while (true) {
            if (!state.advance()) {
                return IterationResult.WAIT;
            }

            for (Row upstreamRow : state) {
                consumeRow(upstreamRow);
            }

            if (state.isDone()) {
                finalizeResult();

                return IterationResult.FETCHED_DONE;
            }
        }
    }

    @Override
    public RowBatch currentBatch0() {
        return res;
    }

    /**
     * Consume a single row.
     *
     * @param row Row.
     */
    private void consumeRow(Row row) {
        List<Object> key = new ArrayList<>(expressions.size());

        for (Expression expression : expressions) {
            key.add(expression.eval(row));
        }

        map.put(new SortKey(key, idx++), row);
    }

    /**
     * Finalize the result.
     */
    private void finalizeResult() {
        if (res != null) {
            return;
        }

        List<Row> resList = new ArrayList<>(map.size());

        resList.addAll(map.values());

        res = new ListRowBatch(resList);
    }

    @Override
    protected void reset1() {
        // TODO: After sorting is completed, there is no need to do a reset since.
        map.clear();

        res = null;

        idx = 0;
    }
}
