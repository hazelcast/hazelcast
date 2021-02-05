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

package com.hazelcast.sql.impl.exec.fetch;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;

import java.util.ArrayList;
import java.util.List;

/**
 * LIMIT/OFFSET core support.
 */
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
        fetchValue = getFetchValue(context, fetch);
        offsetValue = getOffsetValue(context, offset);
    }

    public static long getFetchValue(ExpressionEvalContext context, Expression<?> fetch) {
        return fetch == null ? Long.MAX_VALUE : eval(fetch,  context);
    }

    public static long getOffsetValue(ExpressionEvalContext context, Expression<?> offset) {
        return offset == null ? 0 : eval(offset, context);
    }

    public RowBatch apply(RowBatch batch) {
        if (fetchApplied == fetchValue) {
            return EmptyRowBatch.INSTANCE;
        }

        long toOffset = offsetValue - offsetApplied;

        if (toOffset >= batch.getRowCount()) {
            // Skip the whole batch
            offsetApplied += batch.getRowCount();

            return EmptyRowBatch.INSTANCE;
        }

        // Mark offset as finished and adjust the start index accordingly.
        int startIndex;

        if (toOffset != 0L) {
            offsetApplied = offsetValue;

            // toOffset is smaller than the batch size and casting to int is safe
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

    private static long eval(Expression<?> expression, ExpressionEvalContext context) {
        assert expression != null;

        Object val = expression.eval(FetchRow.INSTANCE, context);
        assert val != null && val instanceof Number;

        long val0 = ((Number) val).longValue();
        assert val0 >= 0;
        return val0;
    }

    private RowBatch slice(RowBatch source, int start, int len) {
        List<Row> rows = new ArrayList<>(len);

        for (int i = start; i < start + len; i++) {
            Row row = source.getRow(i);

            rows.add(row);
        }

        return new ListRowBatch(rows);
    }
}
