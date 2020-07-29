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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.Arrays;

/**
 * Implements evaluation of SQL CASE operator.
 */
public final class CaseExpression<T> implements Expression<T>, IdentifiedDataSerializable {

    private Expression<Boolean>[] conditions;
    private Expression<?>[] results;
    private QueryDataType resultType;

    public CaseExpression() {
        // No-op.
    }

    private CaseExpression(Expression<Boolean>[] conditions, Expression<?>[] results, QueryDataType resultType) {
        this.conditions = conditions;
        this.results = results;
        this.resultType = resultType;
    }

    @SuppressWarnings("unchecked")
    public static CaseExpression<?> create(Expression<?>[] expressions, QueryDataType resultType) {
        // The received expressions are going in the interleaved condition-result
        // order, except the last 'else' result which doesn't have a condition:
        // [condition1, result1, condition2, result2, ..., elseResult].

        // The 'else' result doesn't have a condition, so the number of
        // expressions should be always odd.
        assert expressions.length % 2 == 1;

        // Split the received interleaved expressions into conditions and
        // results.
        int conditionCount = expressions.length / 2;
        Expression<Boolean>[] conditions = new Expression[conditionCount];
        Expression<?>[] results = new Expression[conditionCount + 1];
        for (int i = 0; i < conditionCount; i++) {
            conditions[i] = (Expression<Boolean>) expressions[i * 2];
            results[i] = expressions[i * 2 + 1];
        }

        // Add the 'else' result into the results.
        results[results.length - 1] = expressions[expressions.length - 1];

        return new CaseExpression<>(conditions, results, resultType);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_CASE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        // Test for conditions one-by-one.
        for (int i = 0; i < conditions.length; i++) {
            Expression<Boolean> condition = conditions[i];

            Boolean conditionHolds = condition.eval(row, context);
            if (TernaryLogic.isTrue(conditionHolds)) {
                return (T) results[i].eval(row, context);
            }
        }

        // Return the 'else' result if no conditions were met.
        Expression<?> elseResult = results[results.length - 1];
        return (T) elseResult.eval(row, context);
    }

    @Override
    public QueryDataType getType() {
        return resultType;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(conditions.length);

        for (int i = 0; i < conditions.length; i++) {
            out.writeObject(conditions[i]);
            out.writeObject(results[i]);
        }

        out.writeObject(results[results.length - 1]);

        out.writeObject(resultType);
    }

    @SuppressWarnings("unchecked")
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

        resultType = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CaseExpression<?> that = (CaseExpression<?>) o;

        if (!Arrays.equals(conditions, that.conditions)) {
            return false;
        }

        if (!Arrays.equals(results, that.results)) {
            return false;
        }

        return resultType.equals(that.resultType);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(conditions);
        result = 31 * result + Arrays.hashCode(results);
        result = 31 * result + resultType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CaseExpression{" + "conditions=" + Arrays.toString(conditions) + ", results=" + Arrays.toString(results)
                + ", resultType=" + resultType + '}';
    }

}
