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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.VariExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.StringJoiner;

import static com.hazelcast.sql.impl.expression.string.StringFunctionUtils.asVarchar;

public class ConcatWSFunction extends VariExpression<String> {
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
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_CONCAT_WS;
    }
}
