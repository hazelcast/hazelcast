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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.SymbolExpression;
import com.hazelcast.sql.impl.expression.VariExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.jayway.jsonpath.JsonPath;
import org.apache.calcite.sql.SqlJsonQueryEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlJsonQueryWrapperBehavior;

import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

public class JsonQueryFunction extends VariExpression<HazelcastJsonValue> implements IdentifiedDataSerializable {
    private static final ObjectMapper SERIALIZER = new ObjectMapper();
    private static final int JSON_ARG_INDEX = 0;
    private static final int PATH_INDEX = 1;
    private static final int WRAPPER_BEHAVIOR_INDEX = 2;
    private static final int ON_EMPTY_INDEX = 3;
    private static final int ON_ERROR_INDEX = 4;

    public JsonQueryFunction() { }

    private JsonQueryFunction(Expression<?>[] operands) {
        super(operands);
    }

    public static JsonQueryFunction create(Expression<?>[] operands) {
        return new JsonQueryFunction(operands);
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
        final Object operand0 = operands[JSON_ARG_INDEX].eval(row, context);
        final String json = operand0 instanceof HazelcastJsonValue
                ? operand0.toString()
                : (String) operand0;
        final String path = (String) operands[PATH_INDEX].eval(row, context);

        final SqlJsonQueryWrapperBehavior wrapperBehavior = ((SymbolExpression) operands[WRAPPER_BEHAVIOR_INDEX])
                .getSymbol();
        final SqlJsonQueryEmptyOrErrorBehavior onEmpty = ((SymbolExpression) operands[ON_EMPTY_INDEX])
                .getSymbol();
        final SqlJsonQueryEmptyOrErrorBehavior onError = ((SymbolExpression) operands[ON_ERROR_INDEX])
                .getSymbol();

        if (isNullOrEmpty(path)) {
            return onErrorResponse(onError, QueryException.error("JSON_QUERY path expression is empty"));
        }

        if (isNullOrEmpty(json)) {
            return onEmptyResponse(onEmpty);
        }

        try {
            return wrap(execute(json, path, wrapperBehavior));
        } catch (Exception exception) {
            return onErrorResponse(onError, exception);
        }
    }

    private HazelcastJsonValue onErrorResponse(final SqlJsonQueryEmptyOrErrorBehavior onError, final Exception exception) {
        switch (onError) {
            case ERROR:
                throw QueryException.error("JSON_QUERY failed: ", exception);
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
            default:
            case NULL:
                return null;
        }
    }

    private HazelcastJsonValue wrap(String json) {
        return new HazelcastJsonValue(json);
    }

    private String execute(final String json, final String path, final SqlJsonQueryWrapperBehavior wrapperBehavior) {
        final Object result = JsonPath.read(json, path);
        final String serializedResult = serialize(result);

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
                    throw QueryException.error("JSON_QUERY result is not an array");
                }
                return serializedResult;
        }
    }

    private String serialize(Object value) {
        try {
            return SERIALIZER.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw QueryException.error("Failed to serialize JSON_QUERY result: ", exception);
        }
    }

    private boolean isArray(Object value) {
        return value instanceof List;
    }

    private boolean isArrayOrObject(Object value) {
        return value instanceof Map || value instanceof List;
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.JSON;
    }
}
