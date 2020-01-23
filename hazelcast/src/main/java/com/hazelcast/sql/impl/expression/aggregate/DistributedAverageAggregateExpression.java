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
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;

/**
 * Distributed average.
 */
public class DistributedAverageAggregateExpression<T> extends AggregateExpression<T> {
    /** SUM operand type. */
    protected transient DataType sumOperandType;

    /** SUM operand. */
    private Expression sumOperand;

    /** COUNT operand. */
    private Expression countOperand;

    public DistributedAverageAggregateExpression() {
    }

    public DistributedAverageAggregateExpression(Expression sumOperand, Expression countOperand) {
        super(false);

        this.sumOperand = sumOperand;
        this.countOperand = countOperand;
    }

    @Override
    public void collect(QueryContext ctx, Row row, AggregateCollector collector) {
        Object sumOperandValue = sumOperand.eval(ctx, row);
        Object countOperandValue = countOperand.eval(ctx, row);

        assert sumOperandValue != null;
        assert countOperandValue != null;

        long count = countOperand.getType().getConverter().asBigInt(countOperandValue);

        if (sumOperandType == null) {
            sumOperandType = sumOperand.getType();

            resType = AverageAggregateExpression.returnType(sumOperandType);
        }

        ((AverageAggregateCollector) collector).collectMany(sumOperandValue, sumOperandType, resType, count);
    }

    @Override
    public AggregateCollector newCollector(QueryContext ctx) {
        return new AverageAggregateCollector(false);
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
}
