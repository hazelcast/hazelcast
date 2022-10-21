/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.BiExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.Searchable;
import com.hazelcast.sql.impl.expression.SearchableWithNullAs;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * Implements evaluation of SQL SEARCH predicate. The right argument is always a
 * {@link Searchable}.
 */
public final class SearchPredicate extends BiExpression<Boolean> implements IdentifiedDataSerializable {

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
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_SEARCH;
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
        if (!(right instanceof SearchableWithNullAs) && left == null) {
            // We deserialized pre-5.1.5 version of Range class that doesn't support nulls.
            return null;
        }

        Comparable<?> needle = (Comparable<?>) left;
        Searchable searchable = (Searchable<?>) right;

        return searchable.contains(needle);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.BOOLEAN;
    }
}
