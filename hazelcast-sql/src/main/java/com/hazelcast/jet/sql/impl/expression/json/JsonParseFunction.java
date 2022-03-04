/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.expression.json;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

public class JsonParseFunction extends UniExpressionWithType<HazelcastJsonValue> implements IdentifiedDataSerializable {

    public JsonParseFunction() { }

    private JsonParseFunction(Expression<?> operand) {
        super(operand, QueryDataType.JSON);
    }

    public static JsonParseFunction create(Expression<?> operand) {
        return new JsonParseFunction(operand);
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.JSON_PARSE;
    }

    @Override
    public HazelcastJsonValue eval(final Row row, final ExpressionEvalContext context) {
        final String operand = (String) this.operand.eval(row, context);
        return new HazelcastJsonValue(operand);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.JSON;
    }
}
