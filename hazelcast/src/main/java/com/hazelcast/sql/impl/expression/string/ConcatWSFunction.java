/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.expression.VariExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.hazelcast.sql.impl.expression.string.StringFunctionUtils.asVarchar;

public class ConcatWSFunction extends VariExpression<String> implements IdentifiedDataSerializable {
    public ConcatWSFunction() {
    }

    private ConcatWSFunction(Expression<?>... operands) {
        super(operands);
    }

    public static ConcatWSFunction create(Expression<?>... operands) {
        return new ConcatWSFunction(operands);
    }

    @Override
    public String eval(Row row, ExpressionEvalContext context) {

        String separator = asVarchar(operands[0], row, context);
        //TODO: what to do in this case
        if (separator == null) {
            separator = " ";
        }

        return Arrays.stream(operands)
                .skip(1)
                .map(expression -> asVarchar(expression, row, context))
                .collect(Collectors.joining(separator));
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
