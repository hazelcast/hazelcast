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

package com.hazelcast.sql.impl.exec.fetch;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.type.converter.Converters;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for LIMIT/OFFSET support on different operators.
 */
// TODO: Should we support OFFSET without LIMIT? See constructor invocations - we only check for "fetch == null" at the moment.
public class Fetch {
    private final Expression<?> fetch;
    private final Expression<?> offset;

    private long fetchValue;
    private long offsetValue;

    private long offsetApplied;
    private long fetchApplied;

    public Fetch(Expression<?> fetch, Expression<?> offset) {
        this.fetch = fetch;
        this.offset = offset;
    }

    public void setup(ExpressionEvalContext context) {
        fetchValue = eval(fetch, true, context);
        offsetValue = eval(offset, false, context);
    }

    public RowBatch apply(RowBatch batch) {
        if (fetchApplied == fetchValue) {
            return EmptyRowBatch.INSTANCE;
        }

        long toOffset = offsetValue - offsetApplied;

        if (toOffset >= batch.getRowCount()) {
            // The whole batch should be skipped.
            offsetApplied += batch.getRowCount();

            return EmptyRowBatch.INSTANCE;
        }

        // Mark offset as finished and adjust the start index accordingly.
        int startIndex;

        if (toOffset != 0L) {
            offsetApplied = offsetValue;

            startIndex = (int) toOffset;
        } else {
            startIndex = 0;
        }

        // Calculate the end position.
        long toFetch = fetchValue - fetchApplied;
        long canFetch = batch.getRowCount() - startIndex;

        if (toFetch >= canFetch) {
            fetchApplied += canFetch;

            // Return the whole remainder.
            if (startIndex == 0) {
                return batch;
            } else {
                return slice(batch, startIndex, (int) canFetch);
            }
        } else {
            fetchApplied += toFetch;

            return slice(batch, startIndex, (int) toFetch);
        }
    }

    public boolean isDone() {
        return fetchApplied == fetchValue;
    }

    private long eval(Expression<?> expression, boolean fetch, ExpressionEvalContext context) {
        if (expression == null) {
            return 0L;
        }

        String name = fetch ? "LIMIT" : "OFFSET";

        Object val = expression.eval(FetchRow.INSTANCE, context);

        if (val == null) {
            throw QueryException.error("Value of " + name + " cannot be null");
        }

        long val0;

        if (val instanceof Number) {
            val0 = ((Number) val).longValue();
        } else {
            try {
                val0 = Converters.getConverter(val.getClass()).asBigint(val);
            } catch (Exception e) {
                throw QueryException.error("Cannot convert " + name + " value to number: " + val, e);
            }
        }

        if (val0 < 0L) {
            throw QueryException.error("Value of " + name + " cannot be negative: " + val);
        }

        return val0;
    }

    private RowBatch slice(RowBatch source, int start, int len) {
        // TODO: This should be a part of row batch interface, to support different batch types (e.g. byte[]) in future.
        List<Row> rows = new ArrayList<>(len);

        for (int i = start; i < start + len; i++) {
            Row row = source.getRow(i);

            rows.add(row);
        }

        return new ListRowBatch(rows);
    }
}
