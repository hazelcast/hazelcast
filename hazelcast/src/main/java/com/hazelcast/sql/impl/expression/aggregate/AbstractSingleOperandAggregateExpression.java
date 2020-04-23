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

package com.hazelcast.sql.impl.expression.aggregate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.Objects;

/**
 * Aggregate accumulator which uses only a single input.
 */
public abstract class AbstractSingleOperandAggregateExpression<T> extends AggregateExpression {
    /** Operand. */
    private Expression<?> operand;

    protected AbstractSingleOperandAggregateExpression() {
        // No-op.
    }

    protected AbstractSingleOperandAggregateExpression(Expression<?> operand, QueryDataType resType, boolean distinct) {
        super(resType, distinct);

        this.operand = operand;
    }

    @Override
    public void collect(Row row, AggregateCollector collector, ExpressionEvalContext context) {
        Object operandValue = operand.eval(row, context);

        if (isIgnoreNull() && operandValue == null) {
            return;
        }

        collector.collect(operandValue, operand.getType());
    }

    /**
     * @return {@code True} if NULL values should be ignored and not passed to the collector, {@code false} otherwise.
     */
    protected abstract boolean isIgnoreNull();

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeObject(operand);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        operand = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractSingleOperandAggregateExpression<?> that = (AbstractSingleOperandAggregateExpression<?>) o;

        return Objects.equals(operand, that.operand) && Objects.equals(resType, that.resType) && distinct == that.distinct;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resType, distinct, operand);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{operand=" + operand + ", distinct=" + distinct + '}';
    }
}
