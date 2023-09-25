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

package com.hazelcast.sql.impl.expression.datetime;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.sql.impl.expression.ConcurrentInitialSetCache;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.TriExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.temporal.Temporal;
import java.util.Locale;

public class ToCharFunction extends TriExpression<String> {
    private static final int CACHE_SIZE = 100;
    private transient ConcurrentInitialSetCache<String, Formatter> formatterCache;
    private transient ConcurrentInitialSetCache<String, Locale> localeCache;

    public ToCharFunction() { }

    private ToCharFunction(Expression<?> input, Expression<?> format, Expression<?> locale) {
        super(input, format, locale);
        prepareCache();
    }

    public static ToCharFunction create(Expression<?> input, Expression<?> format, Expression<?> locale) {
        return new ToCharFunction(input, format, locale);
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_TO_CHAR;
    }

    @Override
    public String eval(Row row, ExpressionEvalContext context) {
        Object input = operand1.eval(row, context);
        String format = (String) operand2.eval(row, context);
        Formatter formatter = formatterCache.computeIfAbsent(format,
                input instanceof Temporal ? Formatter::forDates : Formatter::forNumbers);
        Locale locale = operand3 == null ? Locale.US : localeCache.computeIfAbsent(
                (String) operand3.eval(row, context), Locale::forLanguageTag);
        return formatter.format(input, locale);
    }

    private void prepareCache() {
        formatterCache = new ConcurrentInitialSetCache<>(CACHE_SIZE);
        localeCache = new ConcurrentInitialSetCache<>(CACHE_SIZE);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.VARCHAR;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        prepareCache();
    }

    private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
        in.defaultReadObject();
        prepareCache();
    }
}
