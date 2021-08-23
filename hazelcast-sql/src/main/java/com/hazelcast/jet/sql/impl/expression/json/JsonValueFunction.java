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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.VariExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.jayway.jsonpath.JsonPath;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;

import java.math.BigDecimal;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

public class JsonValueFunction<T> extends VariExpressionWithType<T> implements IdentifiedDataSerializable {
    private static final ObjectMapper SERIALIZER = new ObjectMapper();
    // TODO: maybe introduce our own Enum?
    // TODO: serialization, toString, equals-hashcode
    private SqlJsonValueEmptyOrErrorBehavior onEmpty = SqlJsonValueEmptyOrErrorBehavior.NULL;
    private SqlJsonValueEmptyOrErrorBehavior onError = SqlJsonValueEmptyOrErrorBehavior.NULL;

    public JsonValueFunction() { }

    private JsonValueFunction(Expression<?>[] operands,
                              QueryDataType resultType,
                              SqlJsonValueEmptyOrErrorBehavior onEmpty,
                              SqlJsonValueEmptyOrErrorBehavior onError) {

        super(operands, resultType);
        this.onEmpty = onEmpty;
        this.onError = onError;
    }

    public static JsonValueFunction<?> create(Expression<?> json,
                                              Expression<?> path,
                                              QueryDataType resultType,
                                              SqlJsonValueEmptyOrErrorBehavior onEmpty,
                                              SqlJsonValueEmptyOrErrorBehavior onError) {
        final Expression<?>[] operands = new Expression<?>[] {
                json,
                path
        };
        return new JsonValueFunction<>(operands, resultType, onEmpty, onError);
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.JSON_VALUE;
    }

    @Override
    public T eval(final Row row, final ExpressionEvalContext context) {
        final Object operand0 = operands[0].eval(row, context);
        final String json = operand0 instanceof HazelcastJsonValue
                ? operand0.toString()
                : (String) operand0;
        final String path = (String) operands[1].eval(row, context);
        if (isNullOrEmpty(path)) {
            return onErrorResponse(onError, QueryException.error("JSON_VALUE path expression is empty"));
        }

        if (isNullOrEmpty(json)) {
            return onEmptyResponse(onEmpty);
        }

        try {
            return (T) convertResultType(JsonPath.read(json, path));
        } catch (Exception exception) {
            return onErrorResponse(onError, exception);
        }
    }

    private T onErrorResponse(SqlJsonValueEmptyOrErrorBehavior onError, Exception exception) {
        return null;
    }

    private T onEmptyResponse(SqlJsonValueEmptyOrErrorBehavior onEmpty) {
        return null;
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private Object convertResultType(final Object result) {
        if (!(result instanceof Number)) {
            return result;
        }

        final Number number = (Number) result;

        switch (this.resultType.getTypeFamily().getPublicType()) {
            case TINYINT:
                return number.byteValue();
            case SMALLINT:
                return number.shortValue();
            case INTEGER:
                return number.intValue();
            case BIGINT:
                return number.longValue();
            case REAL:
                return number.floatValue();
            case DOUBLE:
                return number.doubleValue();
            case DECIMAL:
                return BigDecimal.valueOf(number.doubleValue());
            default:
                return result;
        }
    }
}
