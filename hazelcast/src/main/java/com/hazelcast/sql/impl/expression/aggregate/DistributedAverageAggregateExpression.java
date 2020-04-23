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
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.util.Eval;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.Objects;

/**
 * Distributed average.
 */
public class DistributedAverageAggregateExpression extends AggregateExpression {
    /** SUM operand. */
    private Expression<?> sumOperand;

    /** COUNT operand. */
    private Expression<?> countOperand;

    public DistributedAverageAggregateExpression() {
        // No-op.
    }

    private DistributedAverageAggregateExpression(Expression<?> sumOperand, Expression<?> countOperand) {
        super(QueryDataType.DOUBLE, false);

        this.sumOperand = sumOperand;
        this.countOperand = countOperand;
    }

    public static DistributedAverageAggregateExpression create(Expression<?> sumOperand, Expression<?> countOperand) {
        return new DistributedAverageAggregateExpression(sumOperand, countOperand);
    }

    @Override
    public void collect(Row row, AggregateCollector collector, ExpressionEvalContext context) {
        Object sum = sumOperand.eval(row, context);
        Long count = Eval.asBigint(countOperand, row, context);

        assert sum != null;
        assert count != null;

        ((AverageAggregateCollector) collector).collectMany(sum, sumOperand.getType(), count);
    }

    @Override
    public AggregateCollector newCollector(QueryFragmentContext ctx) {
        return new AverageAggregateCollector(resType, false);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeObject(sumOperand);
        out.writeObject(countOperand);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        sumOperand = in.readObject();
        countOperand = in.readObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(sumOperand, countOperand);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DistributedAverageAggregateExpression that = (DistributedAverageAggregateExpression) o;

        return sumOperand.equals(that.sumOperand) && countOperand.equals(that.countOperand);
    }

    @Override
    public String toString() {
        return "DistributedAverageAggregateExpression{sumOperand=" + sumOperand + ", countOperand=" + countOperand + '}';
    }
}
