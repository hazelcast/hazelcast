/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.sql.impl.expression.predicate;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * Implements evaluation of SQL IS FALSE predicate.
 */
public final class IsFalsePredicate extends UniExpression<Boolean> {

    public IsFalsePredicate() {
        // No-op.
    }

    private IsFalsePredicate(Expression<?> operand) {
        super(operand);
    }

    public static IsFalsePredicate create(Expression<?> operand) {
        return new IsFalsePredicate(operand);
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_IS_FALSE;
    }

    @Override
    public Boolean eval(Row row, ExpressionEvalContext context) {
        return TernaryLogic.isFalse((Boolean) operand.eval(row, context));
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.BOOLEAN;
    }

}
