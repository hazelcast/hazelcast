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
import com.hazelcast.sql.impl.expression.TriCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

/**
 * SUBSTRING string function.
 */
public class SubstringFunction extends TriCallExpression<String> {
    public SubstringFunction() {
        // No-op.
    }

    private SubstringFunction(Expression<?> source, Expression<?> start, Expression<?> length) {
        super(source, start, length);
    }

    public static SubstringFunction create(Expression<?> source, Expression<?> start, Expression<?> length) {
        source.ensureCanConvertToVarchar();
        start.ensureCanConvertToVarchar();
        length.ensureCanConvertToInt();

        return new SubstringFunction(source, start, length);
    }

    @Override
    public String eval(Row row) {
        String source = operand1.evalAsVarchar(row);
        Integer start = operand2.evalAsInt(row);
        Integer length = operand3.evalAsInt(row);

        return StringExpressionUtils.substring(source, start, length);
    }

    @Override
    public DataType getType() {
        return DataType.VARCHAR;
    }
}
