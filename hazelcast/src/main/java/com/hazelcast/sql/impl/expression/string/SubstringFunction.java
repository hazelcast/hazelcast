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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.util.EnsureConvertible;
import com.hazelcast.sql.impl.expression.util.Eval;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.TriExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * SUBSTRING string function.
 */
public class SubstringFunction extends TriExpression<String> {
    public SubstringFunction() {
        // No-op.
    }

    private SubstringFunction(Expression<?> source, Expression<?> start, Expression<?> length) {
        super(source, start, length);
    }

    public static SubstringFunction create(Expression<?> source, Expression<?> start, Expression<?> length) {
        EnsureConvertible.toVarchar(source);
        EnsureConvertible.toInt(start);
        EnsureConvertible.toInt(length);

        return new SubstringFunction(source, start, length);
    }

    @Override
    public String eval(Row row, ExpressionEvalContext context) {
        String source = Eval.asVarchar(operand1, row, context);
        Integer start = Eval.asInt(operand2, row, context);
        Integer length = Eval.asInt(operand3, row, context);

        return StringExpressionUtils.substring(source, start, length);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.VARCHAR;
    }
}
