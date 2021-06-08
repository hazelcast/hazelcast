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

package com.hazelcast.jet.sql.impl.processors;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.sql.impl.JetQueryResultProducer;
import com.hazelcast.jet.sql.impl.JetSqlCoreBackendImpl;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.impl.JetSqlCoreBackend;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.EmptyRow;

import javax.annotation.Nonnull;
import java.util.concurrent.CancellationException;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.sql.impl.SqlErrorCode.CANCELLED_BY_USER;

public final class RootResultConsumerSink implements Processor {

    private final Expression<?> limitExpression;

    private JetQueryResultProducer rootResultConsumer;

    private RootResultConsumerSink(Expression<?> limitExpression) {
        this.limitExpression = limitExpression;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        NodeEngineImpl nodeEngine = getNodeEngine(context.hazelcastInstance());
        JetSqlCoreBackendImpl jetSqlCoreBackend = nodeEngine.getService(JetSqlCoreBackend.SERVICE_NAME);
        rootResultConsumer = jetSqlCoreBackend.getResultConsumerRegistry().remove(context.jobId());
        assert rootResultConsumer != null;

        ExpressionEvalContext evalContext = SimpleExpressionEvalContext.from(context);

        Number limit = evaluate(limitExpression, evalContext);
        if (limit == null) {
            throw QueryException.error("LIMIT value cannot be null");
        }
        if (limit.longValue() < 0L) {
            throw QueryException.error("LIMIT value cannot be negative: " + limit);
        }

        rootResultConsumer.init(limit.longValue());
    }

    private static Number evaluate(Expression<?> limitExpression, ExpressionEvalContext evalContext) {
        return (Number) limitExpression.eval(EmptyRow.INSTANCE, evalContext);
    }

    @Override
    public boolean tryProcess() {
        try {
            rootResultConsumer.ensureNotDone();
        } catch (QueryException e) {
            if (e.getCode() == CANCELLED_BY_USER) {
                throw new CancellationException();
            }
            throw e;
        }
        return true;
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        try {
            rootResultConsumer.consume(inbox);
        } catch (QueryException e) {
            if (e.getCode() == CANCELLED_BY_USER) {
                throw new CancellationException();
            }
            throw e;
        }
    }

    @Override
    public boolean complete() {
        rootResultConsumer.done();
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    public static ProcessorMetaSupplier rootResultConsumerSink(Address initiatorAddress, Expression<?> limitExpression) {
        ProcessorSupplier pSupplier = ProcessorSupplier.of(() -> new RootResultConsumerSink(limitExpression));
        return forceTotalParallelismOne(pSupplier, initiatorAddress);
    }
}
