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
import com.google.gson.Gson;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.VariExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import org.apache.calcite.sql.SqlJsonQueryEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlJsonQueryWrapperBehavior;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.jet.sql.impl.expression.json.JsonPathUtil.isArrayOrObject;

@SuppressWarnings("checkstyle:MagicNumber")
public class JsonQueryFunction extends VariExpression<HazelcastJsonValue> implements IdentifiedDataSerializable {
    private static final Gson SERIALIZER = new Gson();

    private final Cache<String, JsonPath> pathCache = JsonPathUtil.makePathCache();
    private SqlJsonQueryWrapperBehavior wrapperBehavior;
    private SqlJsonQueryEmptyOrErrorBehavior onEmpty;
    private SqlJsonQueryEmptyOrErrorBehavior onError;

    public JsonQueryFunction() { }

    private JsonQueryFunction(
            Expression<?>[] operands,
            SqlJsonQueryWrapperBehavior wrapperBehavior,
            SqlJsonQueryEmptyOrErrorBehavior onEmpty,
            SqlJsonQueryEmptyOrErrorBehavior onError
    ) {
        super(operands);
        this.wrapperBehavior = wrapperBehavior;
        this.onEmpty = onEmpty;
        this.onError = onError;
    }

    public static JsonQueryFunction create(Expression<?> json,
                                           Expression<?> path,
                                           SqlJsonQueryWrapperBehavior wrapperBehavior,
                                           SqlJsonQueryEmptyOrErrorBehavior onEmpty,
                                           SqlJsonQueryEmptyOrErrorBehavior onError) {
        final Expression<?>[] operands = new Expression<?>[] {
                json,
                path
        };

        return new JsonQueryFunction(operands, wrapperBehavior, onEmpty, onError);
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.JSON_QUERY;
    }

    @Override
    public HazelcastJsonValue eval(final Row row, final ExpressionEvalContext context) {
        // first evaluate the required parameter
        final String path = (String) operands[1].eval(row, context);
        if (path == null) {
            throw QueryException.error("JSONPath expression can not be null");
        }

        final Object operand0 = operands[0].eval(row, context);
        final String json = operand0 instanceof HazelcastJsonValue
                ? operand0.toString()
                : (String) operand0;
        if (isNullOrEmpty(json)) {
            return onEmptyResponse(onEmpty);
        }

        final JsonPath jsonPath;
        try {
            jsonPath = pathCache.asMap().computeIfAbsent(path, JsonPathUtil::compile);
        } catch (InvalidPathException exception) {
            throw QueryException.error("Invalid JSONPath expression: " + exception, exception);
        }

        try {
            return wrap(execute(json, jsonPath, wrapperBehavior));
        } catch (Exception exception) {
             return onErrorResponse(onError, exception);
        }
    }

    private HazelcastJsonValue onErrorResponse(final SqlJsonQueryEmptyOrErrorBehavior onError, final Exception exception) {
        switch (onError) {
            case ERROR:
                throw QueryException.error("JSON_QUERY failed: " + exception, exception);
            case EMPTY_ARRAY:
                return wrap("[]");
            case EMPTY_OBJECT:
                return wrap("{}");
            default:
            case NULL:
                return null;
        }
    }

    private HazelcastJsonValue onEmptyResponse(final SqlJsonQueryEmptyOrErrorBehavior onEmpty) {
        switch (onEmpty) {
            case ERROR:
                throw QueryException.error("Empty JSON object");
            case EMPTY_ARRAY:
                return wrap("[]");
            case EMPTY_OBJECT:
                return wrap("{}");
            case NULL:
            default:
                return null;
        }
    }

    private HazelcastJsonValue wrap(String json) {
        return new HazelcastJsonValue(json);
    }

    private String execute(final String json, final JsonPath path, final SqlJsonQueryWrapperBehavior wrapperBehavior) {
        final Object result = JsonPathUtil.read(json, path);
        final String serializedResult = SERIALIZER.toJson(result);

        switch (wrapperBehavior) {
            case WITH_CONDITIONAL_ARRAY:
                return isArrayOrObject(result)
                        ? serializedResult
                        : "[" + serializedResult + "]";
            case WITH_UNCONDITIONAL_ARRAY:
                return "[" + serializedResult + "]";
            default:
            case WITHOUT_ARRAY:
                if (!isArrayOrObject(result)) {
                    throw QueryException.error("JSON_QUERY result is not an array or object");
                }
                return serializedResult;
        }
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.JSON;
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
        this.onEmpty = SqlJsonQueryEmptyOrErrorBehavior.valueOf(in.readString());
        this.onError = SqlJsonQueryEmptyOrErrorBehavior.valueOf(in.readString());
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

        JsonQueryFunction that = (JsonQueryFunction) o;

        return this.onEmpty.equals(that.onEmpty) && this.onError.equals(that.onError);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
                + "{operand=" + Arrays.toString(operands)
                + ", onEmpty=" + onEmpty.name()
                + ", onError=" + onError.name()
                + '}';
    }
}
