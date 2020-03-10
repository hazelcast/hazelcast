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

import com.hazelcast.sql.impl.expression.BiExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * A function which accepts a string, and return another string.
 */
public class ConcatFunction extends BiExpression<String> {
    public ConcatFunction() {
        // No-op.
    }

    private ConcatFunction(Expression<?> first, Expression<?> second) {
        super(first, second);
    }

    public static ConcatFunction create(Expression<?> first, Expression<?> second) {
        first.ensureCanConvertToVarchar();
        second.ensureCanConvertToVarchar();

        return new ConcatFunction(first, second);
    }

    @Override
    public String eval(Row row) {
        String first = operand1.evalAsVarchar(row);
        String second = operand2.evalAsVarchar(row);

        return StringExpressionUtils.concat(first, second);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.VARCHAR;
    }
}
