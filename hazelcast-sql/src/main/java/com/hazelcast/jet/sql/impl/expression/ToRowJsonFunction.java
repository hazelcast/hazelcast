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

package com.hazelcast.jet.sql.impl.expression;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.impl.util.ReflectionUtils;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.jet.sql.impl.expression.json.JsonCreationUtil;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static java.util.Collections.newSetFromMap;

public class ToRowJsonFunction extends UniExpressionWithType<HazelcastJsonValue> implements IdentifiedDataSerializable {

    public ToRowJsonFunction() { }

    private ToRowJsonFunction(Expression<?> operand) {
        super(operand, QueryDataType.ROW);
    }

    public static ToRowJsonFunction create(Expression<?> operand) {
        return new ToRowJsonFunction(operand);
    }

    @Override
    public HazelcastJsonValue eval(final Row row, final ExpressionEvalContext context) {
        final Object obj = this.operand.eval(row, context);
        final QueryDataType queryDataType = operand.getType();

        if (obj == null) {
            return new HazelcastJsonValue("");
        }

        if (obj instanceof GenericRecord) {
            throw QueryException.error("TO_ROW_JSON function is only supported for Java Types");
        }
        final List<Object> values = new ArrayList<>(queryDataType.getObjectFields().size());
        convert(obj, values, queryDataType, newSetFromMap(new IdentityHashMap<>()));

        return new HazelcastJsonValue(JsonCreationUtil.serializeValue(values));
    }

    private void convert(Object source, List<Object> values, QueryDataType dataType, final Set<Object> seenObjects) {
        if (!seenObjects.add(source)) {
            throw QueryException.error(SqlErrorCode.DATA_EXCEPTION, "Cycle detected in row value");
        }

        for (final QueryDataType.QueryDataTypeField field : dataType.getObjectFields()) {
            final Object fieldValue = ReflectionUtils.getFieldValue(field.getName(), source);
            if (!field.getDataType().isCustomType() || fieldValue == null) {
                values.add(fieldValue);
            } else {
                final List<Object> subRowValues = new ArrayList<>();
                values.add(subRowValues);
                convert(fieldValue, subRowValues, field.getDataType(), seenObjects);
            }
        }
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.TO_ROW_JSON;
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.ROW;
    }
}
