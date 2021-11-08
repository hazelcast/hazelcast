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

import com.google.common.cache.Cache;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.VariExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Objects;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

public class JsonValueFunction<T> extends VariExpressionWithType<T> implements IdentifiedDataSerializable {
    private final Cache<String, JsonPath> pathCache = JsonPathUtil.makePathCache();

    private SqlJsonValueEmptyOrErrorBehavior onEmpty;
    private SqlJsonValueEmptyOrErrorBehavior onError;

    public JsonValueFunction() {
    }

    private JsonValueFunction(
            Expression<?>[] operands,
            QueryDataType resultType,
            SqlJsonValueEmptyOrErrorBehavior onEmpty,
            SqlJsonValueEmptyOrErrorBehavior onError
    ) {
        super(operands, resultType);
        this.onEmpty = onEmpty;
        this.onError = onError;
    }

    public static JsonValueFunction<?> create(
            Expression<?> json,
            Expression<?> path,
            Expression<?> defaultValueOnEmpty,
            Expression<?> defaultValueOnError,
            QueryDataType resultType,
            SqlJsonValueEmptyOrErrorBehavior onEmpty,
            SqlJsonValueEmptyOrErrorBehavior onError
    ) {
        final Expression<?>[] operands = new Expression<?>[]{
                json,
                path,
                defaultValueOnEmpty,
                defaultValueOnError
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
        // first evaluate the required parameter
        final String path = (String) operands[1].eval(row, context);
        if (path == null) {
            throw QueryException.error("JSONPath expression can not be null");
        }

        // needed for further checks, can be a dynamic expression, therefore can not be inlined as part of function args
        final Object defaultOnEmpty = operands[2].eval(row, context);
        final Object defaultOnError = operands[3].eval(row, context);

        final Object operand0 = operands[0].eval(row, context);
        final String json = operand0 instanceof HazelcastJsonValue
                ? operand0.toString()
                : (String) operand0;
        if (isNullOrEmpty(json)) {
            return onEmptyResponse(onEmpty, defaultOnEmpty);
        }

        final JsonPath jsonPath;
        try {
            jsonPath = pathCache.asMap().computeIfAbsent(path, JsonPathUtil::compile);
        } catch (InvalidPathException | IllegalArgumentException exception) {
            throw QueryException.error("Invalid JSONPath expression: " + exception, exception);
        }

        try {
            return execute(json, jsonPath);
        } catch (Exception exception) {
            return onErrorResponse(onError, exception, defaultOnError);
        }
    }

    private T onEmptyResponse(SqlJsonValueEmptyOrErrorBehavior onEmpty, Object defaultValue) {
        switch (onEmpty) {
            case ERROR:
                throw QueryException.error("JSON argument is empty");
            case DEFAULT:
                return (T) defaultValue;
            case NULL:
            default:
                return null;
        }
    }

    private T onErrorResponse(SqlJsonValueEmptyOrErrorBehavior onError, Exception exception, Object defaultValue) {
        switch (onError) {
            case ERROR:
                throw QueryException.error("JSON_VALUE failed: " + exception, exception);
            case DEFAULT:
                return (T) defaultValue;
            case NULL:
            default:
                return null;
        }
    }

    private T execute(final String json, final JsonPath path) {
        final Object result = JsonPathUtil.read(json, path);
        if (JsonPathUtil.isArrayOrObject(result)) {
            throw QueryException.error("Result of JSON_VALUE can not be array or object");
        }
        return (T) convertResultType(result);
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private Object convertResultType(final Object result) {
        if (resultType.getTypeFamily().equals(QueryDataTypeFamily.VARCHAR)) {
            return result.toString();
        }

        switch (this.resultType.getTypeFamily().getPublicType()) {
            case TINYINT:
                return ((Number) result).byteValue();
            case SMALLINT:
                return ((Number) result).shortValue();
            case INTEGER:
                return ((Number) result).intValue();
            case BIGINT:
                return ((Number) result).longValue();
            case REAL:
                return ((Number) result).floatValue();
            case DOUBLE:
                return ((Number) result).doubleValue();
            case DECIMAL:
                Number number = ((Number) result);
                return number instanceof BigDecimal
                        ? (BigDecimal) number
                        : BigDecimal.valueOf(number.doubleValue());
            default:
                return result;
        }
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeString(onEmpty.name());
        out.writeString(onError.name());
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        super.readData(in);
        this.onEmpty = SqlJsonValueEmptyOrErrorBehavior.valueOf(in.readString());
        this.onError = SqlJsonValueEmptyOrErrorBehavior.valueOf(in.readString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), onEmpty, onError);
    }

    @Override
    public boolean equals(final Object o) {
        if (!super.equals(o)) {
            return false;
        }

        JsonValueFunction<?> that = (JsonValueFunction<?>) o;

        return this.onEmpty.equals(that.onEmpty) && this.onError.equals(that.onError);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
                + "{operand=" + Arrays.toString(operands)
                + ", resultType=" + resultType
                + ", onEmpty=" + onEmpty.name()
                + ", onError=" + onError.name()
                + '}';
    }
}
