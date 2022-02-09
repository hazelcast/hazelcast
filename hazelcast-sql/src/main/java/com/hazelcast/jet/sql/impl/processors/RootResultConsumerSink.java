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
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.jet.sql.impl.QueryResultProducerImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.EmptyRow;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.CancellationException;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne;
import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.sql.impl.SqlErrorCode.CANCELLED_BY_USER;

public final class RootResultConsumerSink implements Processor {

    private final Expression<?> limitExpression;
    private final Expression<?> offsetExpression;

    private QueryResultProducerImpl rootResultConsumer;

    private RootResultConsumerSink(Expression<?> limitExpression, Expression<?> offsetExpression) {
        this.limitExpression = limitExpression;
        this.offsetExpression = offsetExpression;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        rootResultConsumer = getNodeEngine(context.hazelcastInstance())
                .getSqlService()
                .getInternalService()
                .getResultRegistry()
                .remove(context.jobId());
        assert rootResultConsumer != null;

        ExpressionEvalContext evalContext = ExpressionEvalContext.from(context);

        Number limit = evaluate(limitExpression, evalContext);
        if (limit == null) {
            throw QueryException.error("LIMIT value cannot be null");
        }
        if (limit.longValue() < 0L) {
            throw QueryException.error("LIMIT value cannot be negative: " + limit);
        }

        Number offset = evaluate(offsetExpression, evalContext);
        if (offset == null) {
            throw QueryException.error("OFFSET value cannot be null");
        }
        if (offset.longValue() < 0L) {
            throw QueryException.error("OFFSET value cannot be negative: " + offset);
        }

        rootResultConsumer.init(limit.longValue(), offset.longValue());
    }

    private static Number evaluate(Expression<?> expression, ExpressionEvalContext evalContext) {
        return (Number) expression.eval(EmptyRow.INSTANCE, evalContext);
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
    public boolean closeIsCooperative() {
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    public static ProcessorMetaSupplier rootResultConsumerSink(
            Address initiatorAddress,
            Expression<?> limitExpression,
            Expression<?> offsetExpression
    ) {
        ProcessorSupplier pSupplier = ProcessorSupplier.of(new Supplier(limitExpression, offsetExpression));
        return forceTotalParallelismOne(pSupplier, initiatorAddress);
    }

    public static class Supplier implements SupplierEx<Processor>, IdentifiedDataSerializable {
        private Expression<?> limitExpression;
        private Expression<?> offsetExpression;

        public Supplier(Expression<?> limitExpression, Expression<?> offsetExpression) {
            this.limitExpression = limitExpression;
            this.offsetExpression = offsetExpression;
        }

        public Supplier() {
        }

        @Override
        public Processor getEx() {
            return new RootResultConsumerSink(limitExpression, offsetExpression);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(limitExpression);
            out.writeObject(offsetExpression);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            limitExpression = in.readObject();
            offsetExpression = in.readObject();
        }

        @Override
        public int getFactoryId() {
            return JetSqlSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return JetSqlSerializerHook.ROOT_RESULT_CONSUMER_SINK_SUPPLIER;
        }
    }
}
