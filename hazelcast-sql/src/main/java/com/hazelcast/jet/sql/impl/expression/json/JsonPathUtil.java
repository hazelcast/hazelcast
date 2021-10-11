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
import com.google.common.cache.CacheBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.hazelcast.query.QueryException;
import com.jayway.jsonpath.InvalidPathException;
import org.jsfr.json.Collector;
import org.jsfr.json.GsonParser;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.ValueBox;
import org.jsfr.json.compiler.JsonPathCompiler;
import org.jsfr.json.path.JsonPath;
import org.jsfr.json.provider.GsonProvider;

import java.util.Collection;
import java.util.stream.Collectors;

public final class JsonPathUtil {
    private static final long CACHE_SIZE = 50L;
    private static final JsonSurfer SURFER = new JsonSurfer(GsonParser.INSTANCE, GsonProvider.INSTANCE);;

    private JsonPathUtil() { }

    public static Cache<String, JsonPath> makePathCache() {
        return CacheBuilder.newBuilder()
                .maximumSize(CACHE_SIZE)
                .build();
    }

    public static JsonPath compile(String path) {
        try {
            return JsonPathCompiler.compile(path);
        } catch (IllegalArgumentException e) {
            throw new InvalidPathException("Illegal argument provided to JSONPath compile function", e);
        }
    }

    // TODO [viliam] remove this?
    public static Object read(String json, String pathString) {
        return read(json, compile(pathString));
    }

    public static Collection<Object> read(String json, JsonPath path) {
        Collector collector = SURFER.collector(json);
        ValueBox<Collection<JsonElement>> box = collector.collectAll(path, JsonElement.class);
        collector.exec();
        return box.get().stream()
                // translate JSON primitives in the result
                .map(result -> {
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
                        throw QueryException.error("Unsupported JSONPath result type: " + primitive.getClass().getName());
                    }
                })
                .collect(Collectors.toList());
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
