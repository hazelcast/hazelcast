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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.VariExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.sql.SqlJsonQueryEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlJsonQueryWrapperBehavior;
import org.jsfr.json.exception.JsonPathCompilerException;
import org.jsfr.json.path.JsonPath;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import static com.hazelcast.jet.sql.impl.expression.json.JsonPathUtil.serialize;
import static com.hazelcast.jet.sql.impl.expression.json.JsonPathUtil.wrapToArray;

@SuppressWarnings("checkstyle:MagicNumber")
public class JsonQueryFunction extends VariExpression<HazelcastJsonValue> implements IdentifiedDataSerializable {
    private static final ILogger LOGGER = Logger.getLogger(JsonQueryFunction.class);

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

    public static JsonQueryFunction create(
            Expression<?> json,
            Expression<?> path,
            SqlJsonQueryWrapperBehavior wrapperBehavior,
            SqlJsonQueryEmptyOrErrorBehavior onEmpty,
            SqlJsonQueryEmptyOrErrorBehavior onError
    ) {
        final Expression<?>[] operands = new Expression<?>[]{json, path};

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
            throw QueryException.error("SQL/JSON path expression cannot be null");
        }

        final Object operand0 = operands[0].eval(row, context);
        String json = operand0 instanceof HazelcastJsonValue
                ? operand0.toString()
                : (String) operand0;
        if (json == null) {
            json = "";
        }

        final JsonPath jsonPath;
        try {
            jsonPath = pathCache.asMap().computeIfAbsent(path, JsonPathUtil::compile);
        } catch (JsonPathCompilerException e) {
            // We deliberately don't use the cause here. The reason is that exceptions from ANTLR are not always
            // serializable, they can contain references to parser context and other objects, which are not.
            // That's why we also log the exception here.
            LOGGER.fine("JSON_QUERY JsonPath compilation failed", e);
            throw QueryException.error("Invalid SQL/JSON path expression: " + e.getMessage());
        }

        return wrap(execute(json, jsonPath));
    }

    private String onErrorResponse(final Exception exception) {
        switch (onError) {
            case ERROR:
                // We deliberately don't use the cause here. The reason is that exceptions from ANTLR are not always
                // serializable, they can contain references to parser context and other objects, which are not.
                // That's why we also log the exception here.
                LOGGER.fine("JSON_QUERY failed", exception);
                throw QueryException.error("JSON_QUERY failed: " + exception);
            case EMPTY_ARRAY:
                return "[]";
            case EMPTY_OBJECT:
                return "{}";
            default:
            case NULL:
                return null;
        }
    }

    private String onEmptyResponse() {
        switch (onEmpty) {
            case ERROR:
                throw QueryException.error("JSON_QUERY evaluated to no value");
            case EMPTY_ARRAY:
                return "[]";
            case EMPTY_OBJECT:
                return "{}";
            case NULL:
            default:
                return null;
        }
    }

    private HazelcastJsonValue wrap(String json) {
        if (json == null) {
            return null;
        }
        return new HazelcastJsonValue(json);
    }

    private String execute(final String json, final JsonPath path) {
        Collection<Object> resultColl;
        try {
            resultColl = JsonPathUtil.read(json, path);
        } catch (Exception exception) {
            return onErrorResponse(exception);
        }
        if (resultColl.isEmpty()) {
            return onEmptyResponse();
        }
        switch (wrapperBehavior) {
            case WITH_CONDITIONAL_ARRAY:
                return wrapToArray(resultColl, false);
            case WITH_UNCONDITIONAL_ARRAY:
                return wrapToArray(resultColl, true);
            default:
            case WITHOUT_ARRAY:
                if (resultColl.size() > 1) {
                    throw QueryException.error("JSON_QUERY evaluated to multiple values");
                }
                Object result = resultColl.iterator().next();
                return serialize(result);
        }
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.JSON;
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(wrapperBehavior.ordinal());
        out.writeInt(onEmpty.ordinal());
        out.writeInt(onError.ordinal());
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        super.readData(in);
        this.wrapperBehavior = SqlJsonQueryWrapperBehavior.values()[in.readInt()];
        this.onEmpty = SqlJsonQueryEmptyOrErrorBehavior.values()[in.readInt()];
        this.onError = SqlJsonQueryEmptyOrErrorBehavior.values()[in.readInt()];
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
