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
import org.jsfr.json.Collector;
import org.jsfr.json.JacksonJrParser;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.ValueBox;
import org.jsfr.json.compiler.JsonPathCompiler;
import org.jsfr.json.path.JsonPath;
import org.jsfr.json.provider.JacksonJrProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public final class JsonPathUtil {
    private static final long CACHE_SIZE = 50L;
    private static final JsonSurfer SURFER = new JsonSurfer(new JacksonJrParser(), JacksonJrProvider.INSTANCE);;

    private JsonPathUtil() { }

    public static Cache<String, JsonPath> makePathCache() {
        return CacheBuilder.newBuilder()
                .maximumSize(CACHE_SIZE)
                .build();
    }

    public static JsonPath compile(String path) {
        return JsonPathCompiler.compile(path);
    }

    // TODO [viliam] remove this?
    public static Object read(String json, String pathString) {
        return read(json, compile(pathString));
    }

    public static Collection<Object> read(String json, JsonPath path) {
        Collector collector = SURFER.collector(json);
        ValueBox<Collection<Object>> box = collector.collectAll(path, Object.class);
        collector.exec();
        return box.get();
    }

    public static boolean isArray(Object value) {
        return value instanceof ArrayList;
    }

    public static boolean isObject(Object value) {
        return value instanceof Map;
    }

    public static boolean isArrayOrObject(Object value) {
        return isArray(value) || isObject(value);
    }
}
