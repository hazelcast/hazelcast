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

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * Not predicate.
 */
public class NotPredicate extends UniExpression<Boolean> {

    public NotPredicate() {
        // No-op.
    }

    public NotPredicate(Expression<?> operand) {
        super(operand);
    }

    public static NotPredicate create(Expression<?> operand) {
        return new NotPredicate(operand);
    }

    @Override
    public Boolean eval(Row row, ExpressionEvalContext context) {
        return TernaryLogic.not((Boolean) operand.eval(row, context));
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.BOOLEAN;
    }

}
