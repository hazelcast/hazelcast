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
import com.hazelcast.sql.impl.expression.AbstractSarg;
import com.hazelcast.sql.impl.expression.BiExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.SargExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * Implements evaluation of SQL SEARCH predicate. The right argument is always a
 * {@link SargExpression}.
 */
public final class SearchPredicate extends BiExpression<Boolean> {

    public SearchPredicate() {
    }

    private SearchPredicate(Expression<?> left, Expression<?> right) {
        super(left, right);
    }

    public static SearchPredicate create(Expression<?> left, Expression<?> right) {
        assert left.getType().equals(right.getType());
        return new SearchPredicate(left, right);
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.SEARCH_PREDICATE;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Boolean eval(Row row, ExpressionEvalContext context) {
        Object left = operand1.eval(row, context);
        // if left operand is null, we still proceed with the right operand, because it can contain a match to a NULL

        Object right = operand2.eval(row, context);
        if (right == null) {
            return null;
        }

        Comparable<?> needle = (Comparable<?>) left;
        AbstractSarg sarg = (AbstractSarg<?>) right;

        return sarg.contains(needle);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.BOOLEAN;
    }
}
