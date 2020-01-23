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

package com.hazelcast.sql.impl.expression.predicate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.CallExpression;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;
import java.util.List;

/**
 * CASE-WHEN expression.
 */
public class CaseExpression<T> implements CallExpression<T> {
    /** Conditions. */
    private Expression[] conditions;

    /** Results. */
    private Expression[] results;

    /** Return type. */
    private DataType resType;

    public CaseExpression() {
        // No-op.
    }

    public CaseExpression(List<Expression> expressions) {
        assert expressions != null;
        assert expressions.size() % 2 == 1;

        int conditionCount = expressions.size() / 2;

        conditions = new Expression[conditionCount];
        results = new Expression[conditionCount + 1];

        int idx = 0;

        for (int i = 0; i < conditionCount; i++) {
            conditions[i] = expressions.get(idx++);
            results[i] = expressions.get(idx++);
        }

        // Last expression might be null.
        results[results.length - 1] = expressions.size() == idx + 1 ? expressions.get(idx) : null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(QueryContext ctx, Row row) {
        for (int i = 0; i < conditions.length; i++) {
            Expression<Boolean> condition = conditions[i];

            Boolean conditionRes = condition.eval(ctx, row);

            if (conditionRes != null && conditionRes) {
                return getResult(results[i], ctx, row);
            }
        }

        return getResult(results[results.length - 1], ctx, row);
    }

    /**
     * Get result of matching condition.
     *
     * @param operand Result expression.
     * @param ctx Context.
     * @param row Row.
     * @return Result.
     */
    @SuppressWarnings("unchecked")
    private T getResult(Expression operand, QueryContext ctx, Row row) {
        if (operand == null) {
            return null;
        }

        Object operandValue = operand.eval(ctx, row);

        if (operandValue == null) {
            return null;
        }

        if (resType == null) {
            resType = operand.getType();
        }

        return (T) operandValue;
    }

    @Override
    public int operator() {
        return CallOperator.CASE;
    }

    @Override
    public DataType getType() {
        return DataType.notNullOrLate(resType);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(conditions.length);

        for (int i = 0; i < conditions.length; i++) {
            out.writeObject(conditions[i]);
            out.writeObject(results[i]);
        }

        out.writeObject(results[results.length - 1]);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();

        conditions = new Expression[len];
        results = new Expression[len + 1];

        for (int i = 0; i < len; i++) {
            conditions[i] = in.readObject();
            results[i] = in.readObject();
        }

        results[len] = in.readObject();
    }
}
