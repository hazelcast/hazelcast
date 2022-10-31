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

package com.hazelcast.sql.impl.expression.datetime;

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.ConcurrentInitialSetCache;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.TriExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

public class ToCharFunction extends TriExpression<String> implements IdentifiedDataSerializable {
    private static final int CACHE_SIZE = 100;
    private transient ConcurrentInitialSetCache<String, Formatter> formatterCache;
    private Formatter constantFormatterCache;

    public ToCharFunction() { }

    private ToCharFunction(Expression<?> input, Expression<?> format, Expression<?> locale) {
        super(input, format, locale);
        prepareCache();
    }

    public static ToCharFunction create(Expression<?> input, Expression<?> format, Expression<?> locale) {
        return new ToCharFunction(input, format, locale);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_TO_CHAR;
    }

    @Override
    public String eval(Row row, ExpressionEvalContext context) {
        Formatter formatter;
        if (constantFormatterCache != null) {
            formatter = constantFormatterCache;
        } else {
            String format = (String) operand2.eval(row, context);
            formatter = formatterCache.computeIfAbsent(format, Formatter::new);
        }

        Object input = operand1.eval(row, context);
        String locale = (String) operand3.eval(row, context);
        return formatter.format(input, locale);
    }

    private void prepareCache() {
        if (operand2 instanceof ConstantExpression<?>) {
            String format = (String) operand2.eval(null, null);
            constantFormatterCache = new Formatter(format);
        } else {
            formatterCache = new ConcurrentInitialSetCache<>(CACHE_SIZE);
        }
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.VARCHAR;
    }
}
