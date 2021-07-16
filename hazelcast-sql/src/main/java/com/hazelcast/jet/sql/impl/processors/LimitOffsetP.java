/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.processors;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.EmptyRow;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;

public final class LimitOffsetP extends AbstractProcessor {
    private final Expression<?> limitExpression;
    private final Expression<?> offsetExpression;

    private long limit;
    private long offset;

    private LimitOffsetP(Expression<?> limitExpression, Expression<?> offsetExpression) {
        this.limitExpression = limitExpression;
        this.offsetExpression = offsetExpression;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        ExpressionEvalContext evalContext = SimpleExpressionEvalContext.from(context);

        Number limit = evaluate(limitExpression, evalContext);
        if (limit == null) {
            throw QueryException.error("LIMIT value cannot be null");
        }
        if (limit.longValue() < 0L) {
            throw QueryException.error("LIMIT value cannot be negative: " + limit);
        }
        this.limit = limit.longValue();

        Number offset = evaluate(offsetExpression, evalContext);
        if (offset == null) {
            throw QueryException.error("OFFSET value cannot be null");
        }
        if (offset.longValue() < 0L) {
            throw QueryException.error("OFFSET value cannot be negative: " + offset);
        }
        this.offset = offset.longValue();
    }

    @SuppressWarnings("emptyBlock")
    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        while (offset > 0 && inbox.poll() != null) {
            offset--;
        }

        Object row;
        while (limit >= 1 && (row = inbox.peek()) != null) {
            if (tryEmit(-1, row)) {
                inbox.remove();
                if (limit != Long.MAX_VALUE) {
                    limit--;
                }
            }
        }

        while (limit == 0 && inbox.poll() != null) { }
    }

    @Override
    public boolean complete() {
        return super.complete();
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    public static ProcessorMetaSupplier limitOffset(
            Address initiatorAddress,
            Expression<?> limitExpression,
            Expression<?> offsetExpression
    ) {
        ProcessorSupplier pSupplier = ProcessorSupplier.of(() -> new LimitOffsetP(limitExpression, offsetExpression));
        return forceTotalParallelismOne(pSupplier, initiatorAddress);
    }

    private static Number evaluate(Expression<?> expression, ExpressionEvalContext evalContext) {
        return (Number) expression.eval(EmptyRow.INSTANCE, evalContext);
    }
}
