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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.VariExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.sql.SqlJsonConstructorNullClause;

import java.io.IOException;

public class JsonArrayFunction extends VariExpression<HazelcastJsonValue> implements IdentifiedDataSerializable {
    private SqlJsonConstructorNullClause nullClause;

    public JsonArrayFunction() { }

    private JsonArrayFunction(final Expression<?>[] operands, final SqlJsonConstructorNullClause nullClause) {
        super(operands);
        this.nullClause = nullClause;
    }

    public static Expression<?> create(final Expression<?>[] operands, final SqlJsonConstructorNullClause nullClause) {
        return new JsonArrayFunction(operands, nullClause);
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.JSON_ARRAY;
    }

    @Override
    public HazelcastJsonValue eval(final Row row, final ExpressionEvalContext context) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        boolean isFirst = true;
        for (Expression<?> operand : operands) {
            Object result = operand.eval(row, context);
            if (result == null && !keepNulls()) {
                continue;
            }
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(',');
            }

            sb.append(JsonCreationUtil.serializeValue(result));
        }
        sb.append(']');

        return new HazelcastJsonValue(sb.toString());
    }

    private boolean keepNulls() {
        return !nullClause.equals(SqlJsonConstructorNullClause.ABSENT_ON_NULL);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.JSON;
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeString(nullClause.name());
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        super.readData(in);
        this.nullClause = SqlJsonConstructorNullClause.valueOf(in.readString());
    }
}
