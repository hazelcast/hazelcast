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

import com.hazelcast.sql.impl.expression.BiCallExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

/**
 * OR predicate.
 */
public class OrPredicate extends BiCallExpression<Boolean> {
    public OrPredicate() {
        // No-op.
    }

    private OrPredicate(Expression<?> first, Expression<?> second) {
        super(first, second);
    }

    public static OrPredicate create(Expression<?> first, Expression<?> second) {
        first.ensureCanConvertToBit();
        second.ensureCanConvertToBit();

        return new OrPredicate(first, second);
    }

    @Override
    public Boolean eval(Row row) {
        Boolean firstValue = operand1.evalAsBit(row);
        Boolean secondValue = operand2.evalAsBit(row);

        return PredicateExpressionUtils.or(firstValue, secondValue);
    }

    @Override
    public DataType getType() {
        return DataType.BIT;
    }
}
