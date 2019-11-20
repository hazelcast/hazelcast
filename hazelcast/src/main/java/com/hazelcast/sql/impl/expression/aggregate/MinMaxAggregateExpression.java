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

package com.hazelcast.sql.impl.expression.aggregate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;

/**
 * MIN/MAX expression.
 */
public class MinMaxAggregateExpression extends SingleAggregateExpression<Comparable> {
    /** Whether this is MIN. */
    private boolean min;

    public MinMaxAggregateExpression() {
        // No-op.
    }

    public MinMaxAggregateExpression(boolean min, boolean distinct, Expression operand) {
        super(distinct, operand);

        this.min = min;
    }

    @Override
    public AggregateCollector newCollector(QueryContext ctx) {
        return new MinMaxAggregateCollector(min);
    }

    @Override
    protected DataType resolveReturnType(DataType operandType) {
        return operandType;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeBoolean(min);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        min = in.readBoolean();
    }

    @Override
    public String toString() {
        return getClass() + "{min=" + min + ", distinct=" + distinct + '}';
    }
}
