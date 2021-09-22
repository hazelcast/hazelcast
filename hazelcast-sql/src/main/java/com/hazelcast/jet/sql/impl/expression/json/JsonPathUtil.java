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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.hazelcast.query.QueryException;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class JsonPathUtil {
    private static final ParseContext CONTEXT = JsonPath.using(Configuration.builder()
            .jsonProvider(new GsonJsonProvider())
            .build());

    private JsonPathUtil() { }

    public static Object read(String json, String path) {
        final JsonElement result = CONTEXT.parse(json).read(path);
        if (result.isJsonArray() || result.isJsonObject()) {
            return result;
        }

        if (result.isJsonNull()) {
            return null;
        }

        final JsonPrimitive primitive = result.getAsJsonPrimitive();
        if (primitive.isNumber()) {
            final boolean isFPNumber = !primitive.toString().matches("([-]?[1-9]+[0-9]*)|([0])");
            return isFPNumber
                    ? convertFPNumber(primitive.getAsBigDecimal())
                    : convertIntegerNumber(primitive.getAsBigInteger());
        } else if (primitive.isBoolean()) {
            return primitive.getAsBoolean();
        } else if (primitive.isString()) {
            return primitive.getAsString();
        } else {
            throw new QueryException("Unsupported JSONPath result type: " + primitive.getClass().getName());
        }
    }

    public static boolean isArray(Object value) {
        return value instanceof JsonArray;
    }

    public static boolean isObject(Object value) {
        return value instanceof JsonObject;
    }

    public static boolean isArrayOrObject(Object value) {
        return isArray(value) || isObject(value);
    }

    private static Object convertFPNumber(BigDecimal number) {
        final double value = number.doubleValue();
        if (value >= Float.MIN_VALUE && value <= Float.MAX_VALUE) {
            return number.floatValue();
        } else if (value >= Double.MIN_VALUE && value <= Double.MAX_VALUE) {
            return number.doubleValue();
        }
        return number;
    }

    private static Object convertIntegerNumber(BigInteger number) {
        final long value;
        try {
            value = number.longValueExact();
        } catch (ArithmeticException ignored) {
            return number;
        }

        if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
            return number.byteValueExact();
        } else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
            return number.shortValueExact();
        } else if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
            return number.intValueExact();
        } else {
            return value;
        }
    }
}
