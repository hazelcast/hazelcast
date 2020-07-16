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

package com.hazelcast.sql.impl.calcite.opt.physical.index;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.VariExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressWarnings("rawtypes")
public class IndexVariExpression extends VariExpression<Comparable[]> {
    public IndexVariExpression() {
        // No-op.
    }

    public IndexVariExpression(Expression<?>... operands) {
        super(operands);
    }

    @Override
    public Comparable[] eval(Row row, ExpressionEvalContext context) {
        Comparable[] res = new Comparable[operands.length];

        int pos = 0;

        for (Expression<?> operand : operands) {
            Object value = operand.eval(row, context);

            if (value == null) {
                return null;
            }

            // TODO: Be careful with cast!
            res[pos++] = (Comparable) value;
        }

        return res;
    }

    @Override
    public QueryDataType getType() {
        // TODO: Is it valid? Or may be we need some for of inference here?
        return operands[0].getType();
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "Internal usage")
    public Expression<?>[] getOperands() {
        return operands;
    }

    // TODO: Identified data serializable
}
