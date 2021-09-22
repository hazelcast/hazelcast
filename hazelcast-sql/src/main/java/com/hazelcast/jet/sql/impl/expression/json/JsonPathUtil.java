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
import com.jayway.jsonpath.spi.cache.CacheProvider;
import com.jayway.jsonpath.spi.cache.LRUCache;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;

public final class JsonPathUtil {
    private static final int CACHE_ENTRIES = 1000;
    private static final ParseContext CONTEXT = JsonPath.using(Configuration.builder()
            .jsonProvider(new GsonJsonProvider())
            .build());

    static {
        CacheProvider.setCache(new LRUCache(CACHE_ENTRIES));
    }

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
            // GSON's getAsNumber returns LazilyParsedNumber, which produces undesirable serialization results
            // e.g. {"value":3"} instead of plain "3", therefore we need to convert it to plain Number.
            if (primitive.toString().matches(".*[.eE].*")) {
                return primitive.getAsBigDecimal();
            } else {
                return primitive.getAsLong();
            }
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
}
