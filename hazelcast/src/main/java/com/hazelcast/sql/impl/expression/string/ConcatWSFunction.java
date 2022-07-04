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
import com.hazelcast.sql.impl.expression.VariExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.StringJoiner;

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

        if (separator == null) {
            return null;
        }

        StringJoiner joiner = new StringJoiner(separator);
        for (int i = 1; i < operands.length; i++) {
            Object val = operands[i].eval(row, context);
            if (val != null) {
                joiner.add(operands[i].getType().getConverter().asVarchar(val));
            }
        }
        return joiner.toString();
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
        return SqlDataSerializerHook.EXPRESSION_CONCAT_WS;
    }
}
