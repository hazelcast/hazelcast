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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.predicate.TernaryLogic;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class CaseExpression<T> implements Expression<T>, IdentifiedDataSerializable {
    private Expression<Boolean>[] whenExpressions;
    private Expression<?>[] thenExpressions;
    private Expression<?> elseExpression;
    private QueryDataType resultType;

    public CaseExpression() {
    }

    private CaseExpression(Expression<Boolean>[] whenExpressions,
                           Expression<?>[] thenExpressions,
                           Expression<?> elseExpression,
                           QueryDataType resultType) {
        this.whenExpressions = whenExpressions;
        this.thenExpressions = thenExpressions;
        this.elseExpression = elseExpression;
        this.resultType = resultType;
    }

    @SuppressWarnings("unchecked")
    public static <T> CaseExpression<T> create(Expression<?>[] operands, QueryDataType resultType) {
        int branchesSize = operands.length / 2;
        Expression<Boolean>[] whenExpressions = new Expression[branchesSize];
        Expression<?>[] thenExpressions = new Expression[branchesSize];
        int index = 0;
        for (int i = 0; i < operands.length - 1; i += 2) {
            whenExpressions[index] = (Expression<Boolean>) operands[i];
            thenExpressions[index] = operands[i + 1];
            index += 1;
        }
        return new CaseExpression<>(whenExpressions, thenExpressions, operands[operands.length - 1], resultType);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_CASE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(whenExpressions.length);

        for (int i = 0; i < whenExpressions.length; i++) {
            out.writeObject(whenExpressions[i]);
            out.writeObject(thenExpressions[i]);
        }

        out.writeObject(elseExpression);
        out.writeObject(resultType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int branchesSize = in.readInt();

        whenExpressions = new Expression[branchesSize];
        thenExpressions = new Expression[branchesSize];

        for (int i = 0; i < branchesSize; i++) {
            whenExpressions[i] = in.readObject();
            thenExpressions[i] = in.readObject();
        }

        elseExpression = in.readObject();
        resultType = in.readObject();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        for (int i = 0; i < whenExpressions.length; i++) {
            Expression<Boolean> condition = whenExpressions[i];

            Boolean conditionHolds = condition.eval(row, context);
            if (TernaryLogic.isTrue(conditionHolds)) {
                return (T) thenExpressions[i].eval(row, context);
            }
        }

        return (T) elseExpression.eval(row, context);
    }

    @Override
    public QueryDataType getType() {
        return resultType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CaseExpression that = (CaseExpression) o;
        return Arrays.equals(whenExpressions, that.whenExpressions)
                && Arrays.equals(thenExpressions, that.thenExpressions)
                && Objects.equals(elseExpression, that.elseExpression)
                && Objects.equals(resultType, that.resultType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(whenExpressions), Arrays.hashCode(thenExpressions), elseExpression, resultType);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("CaseExpression{");
        int len = whenExpressions.length;
        for (int i = 0; i < len; i++) {
            builder.append("\nwhen ").append(whenExpressions[i])
                    .append(" then").append(thenExpressions[i]);
        }
        builder.append("\nresultedType=").append(resultType);
        builder.append('}');
        return builder.toString();
    }
}
