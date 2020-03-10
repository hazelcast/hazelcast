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

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.TriExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * REPLACE string function.
 */
public class ReplaceFunction extends TriExpression<String> {
    public ReplaceFunction() {
        // No-op.
    }

    private ReplaceFunction(Expression<?> source, Expression<?> search, Expression<?> replacement) {
        super(source, search, replacement);
    }

    public static ReplaceFunction create(Expression<?> source, Expression<?> search, Expression<?> replacement) {
        source.ensureCanConvertToVarchar();
        search.ensureCanConvertToVarchar();
        replacement.ensureCanConvertToVarchar();

        return new ReplaceFunction(source, search, replacement);
    }

    @Override
    public String eval(Row row) {
        String source = operand1.evalAsVarchar(row);
        String search = operand2.evalAsVarchar(row);
        String replacement = operand3.evalAsVarchar(row);

        return StringExpressionUtils.replace(source, search, replacement);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.VARCHAR;
    }
}
