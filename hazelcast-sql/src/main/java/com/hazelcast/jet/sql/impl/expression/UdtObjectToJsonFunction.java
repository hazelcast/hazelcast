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

package com.hazelcast.jet.sql.impl.expression;

import com.hazelcast.core.HazelcastException;
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

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.newSetFromMap;

public class UdtObjectToJsonFunction extends UniExpressionWithType<HazelcastJsonValue> implements IdentifiedDataSerializable {

    public UdtObjectToJsonFunction() { }

    private UdtObjectToJsonFunction(Expression<?> operand) {
        super(operand, QueryDataType.JSON);
    }

    public static UdtObjectToJsonFunction create(Expression<?> operand) {
        return new UdtObjectToJsonFunction(operand);
    }

    @Override
    public HazelcastJsonValue eval(final Row row, final ExpressionEvalContext context) {
        final Object obj = this.operand.eval(row, context);
        final QueryDataType queryDataType = operand.getType();

        if (obj == null) {
            return null;
        }

        final Map<String, Object> value = new HashMap<>();
        if (obj instanceof GenericRecord) {
            convertGenericRecord((GenericRecord) obj, queryDataType, value);
        } else {
            convertPojo(obj, value, queryDataType, newSetFromMap(new IdentityHashMap<>()));
        }

        return new HazelcastJsonValue(JsonCreationUtil.serializeValue(value));
    }

    private void convertPojo(Object source, Map<String, Object> values, QueryDataType dataType, final Set<Object> seenObjects) {
        if (!seenObjects.add(source)) {
            throw QueryException.error(SqlErrorCode.DATA_EXCEPTION, "Cycle detected in row value");
        }

        for (final QueryDataType.QueryDataTypeField field : dataType.getObjectFields()) {
            final Object fieldValue = ReflectionUtils.getFieldValue(field.getName(), source);
            if (!field.getDataType().isCustomType() || fieldValue == null) {
                values.put(field.getName(), fieldValue);
            } else {
                final Map<String, Object> subFieldValue = new HashMap<>();
                values.put(field.getName(), subFieldValue);
                convertPojo(fieldValue, subFieldValue, field.getDataType(), seenObjects);
            }
        }
    }

    private void convertGenericRecord(GenericRecord source, QueryDataType dataType, Map<String, Object> values) {
        for (final QueryDataType.QueryDataTypeField field : dataType.getObjectFields()) {
            final Object value;
            switch (field.getDataType().getTypeFamily()) {
                case VARCHAR:
                    value = source.getString(field.getName());
                    break;
                case BOOLEAN:
                    value = source.getBoolean(field.getName());
                    break;
                case TINYINT:
                    value = source.getInt8(field.getName());
                    break;
                case SMALLINT:
                    value = source.getInt16(field.getName());
                    break;
                case INTEGER:
                    value = source.getInt32(field.getName());
                    break;
                case BIGINT:
                    value = source.getInt64(field.getName());
                    break;
                case DECIMAL:
                    value = source.getDecimal(field.getName());
                    break;
                case REAL:
                    value = source.getFloat32(field.getName());
                    break;
                case DOUBLE:
                    value = source.getFloat64(field.getName());
                    break;
                case TIME:
                    value = source.getTime(field.getName());
                    break;
                case DATE:
                    value = source.getDate(field.getName());
                    break;
                case TIMESTAMP:
                    value = source.getTimestamp(field.getName());
                    break;
                case TIMESTAMP_WITH_TIME_ZONE:
                    value = source.getTimestampWithTimezone(field.getName());
                    break;
                case OBJECT:
                    if (field.getDataType().isCustomType()) {
                        value = new HashMap<String, Object>();
                        convertGenericRecord(
                                source.getGenericRecord(field.getName()),
                                field.getDataType(),
                                (Map<String, Object>) value
                        );
                    } else {
                        throw new HazelcastException("Unsupported field type " + field.getDataType()
                                + " for field " + field.getName());
                    }
                    break;
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_DAY_SECOND:
                case MAP:
                case JSON:
                case ROW:
                case NULL:
                default:
                    throw new HazelcastException("Unsupported field type " + field.getDataType()
                            + " for field " + field.getName());
            }

            values.put(field.getName(), value);
        }
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.UDT_OBJECT_TO_JSON;
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.JSON;
    }
}
