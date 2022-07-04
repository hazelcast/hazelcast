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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.TriExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

public class ReplaceFunction extends TriExpression<String> implements IdentifiedDataSerializable {
    public ReplaceFunction() { }

    private ReplaceFunction(Expression<?> original, Expression<?> from, Expression<?> to) {
        super(original, from, to);
    }

    public static ReplaceFunction create(Expression<?> original, Expression<?> from, Expression<?> to) {
        return new ReplaceFunction(original, from, to);
    }

    @Override
    public String eval(Row row, ExpressionEvalContext context) {
        String original = StringFunctionUtils.asVarchar(operand1, row, context);
        if (original == null) {
            return null;
        }
        String from = StringFunctionUtils.asVarchar(operand2, row, context);
        if (from == null) {
            return null;
        }
        String to = StringFunctionUtils.asVarchar(operand3, row, context);
        if (to == null) {
            return null;
        }
        return StringFunctionUtils.replace(original, from, to);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.VARCHAR;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_REPLACE;
    }
}
