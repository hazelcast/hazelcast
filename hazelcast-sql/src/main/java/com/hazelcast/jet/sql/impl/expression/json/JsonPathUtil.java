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

import com.fasterxml.jackson.jr.ob.JSON;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.sql.impl.QueryException;
import org.jsfr.json.Collector;
import org.jsfr.json.DefaultErrorHandlingStrategy;
import org.jsfr.json.ErrorHandlingStrategy;
import org.jsfr.json.JacksonJrParser;
import org.jsfr.json.JsonSurfer;
import org.jsfr.json.ValueBox;
import org.jsfr.json.compiler.JsonPathCompiler;
import org.jsfr.json.exception.JsonPathCompilerException;
import org.jsfr.json.exception.JsonSurfingException;
import org.jsfr.json.path.JsonPath;
import org.jsfr.json.provider.JacksonJrProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public final class JsonPathUtil {
    private static final ILogger LOGGER = Logger.getLogger(JsonPathUtil.class);
    private static final int CACHE_SIZE = 100;
    private static final ErrorHandlingStrategy ERROR_HANDLING_STRATEGY = new DefaultErrorHandlingStrategy() {
        @Override
        public void handleParsingException(Exception e) {
            // We deliberately do not add `e.getMessage` to the message of the exception thrown here.
            // The reason is that it might contain user data, and the error messages should not contain
            // user data. However, we add it to the cause so that it might still appear in member logs, but
            // we need this to investigate issues. We're only preventing it from being sent to the client and
            // to the application logs. This is a compromise.
            throw new JsonSurfingException("Failed to parse JSON document", e);
        }
    };
    private static final JsonSurfer SURFER =
            new JsonSurfer(new JacksonJrParser(), JacksonJrProvider.INSTANCE, ERROR_HANDLING_STRATEGY);

    private JsonPathUtil() { }

    public static ConcurrentInitialSetCache<String, JsonPath> makePathCache() {
        return new ConcurrentInitialSetCache<>(CACHE_SIZE);
    }

    public static JsonPath compile(String path) {
        try {
            return JsonPathCompiler.compile(path);
        } catch (JsonPathCompilerException e) {
            // We deliberately don't use the cause here. The reason is that exceptions from ANTLR are not always
            // serializable, they can contain references to parser context and other objects, which are not.
            // That's why we also log the exception here.
            LOGGER.fine("JSON_QUERY JsonPath compilation failed", e);
            throw QueryException.error("Invalid SQL/JSON path expression: " + e.getMessage());
        }
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

    public static String wrapToArray(Collection<Object> resultColl, boolean unconditionally) {
        if (resultColl.size() > 1) {
            return serialize(resultColl);
        }
        if (resultColl.isEmpty()) {
            return "[]";
        }
        Object result = resultColl.iterator().next();
        final String serializedResult = serialize(result);
        if (unconditionally) {
            return "[" + serializedResult + "]";
        } else {
            return serializedResult;
        }
    }

    public static String serialize(Object object) {
        try {
            return JSON.std.asString(object);
        } catch (IOException e) {
            // should not happen
            throw new RuntimeException(e);
        }
    }

    /**
     * Implementation of fixed-capacity cache based on {@link ConcurrentHashMap}
     * caching the initial set of keys.
     * <p>
     * The cache has no eviction policy, once an element is put into it, it stays
     * there so long as the cache exists. Once the cache is full, no new items are
     * cached.
     * <p>
     * It's designed for caching of compiled JSONPath expressions in the context of
     * one query execution, based on the assumption that typically there's a low
     * number of distinct expressions that fit into the cache and that the
     * expressions come in arbitrary order. If there number of distinct expressions
     * is larger than capacity, we assume that some are more common than others, and
     * we're likely to observe those at the beginning and cache those. If the number
     * of expressions exceeds the capacity many times, we'll cache arbitrary few of
     * them and the rest will be calculated each time without caching - a similar
     * behavior to what an LRU cache will provide, but without the overhead of usage
     * tracking. Degenerate case is when items are sorted by the cache key - after
     * the initial phase the cache will have zero hit rate.
     * <p>
     * Note: The size of the inner map may become bigger than maxCapacity if there
     * are multiple concurrent computeIfAbsent executions. We don't address this for
     * the purpose of optimizing the read performance. The amount the size can
     * exceed the limit is bounded by the number of concurrent writers.
     */
    public static class ConcurrentInitialSetCache<K, V> {
        // package-visible for tests
        final Map<K, V> cache;
        private final int capacity;

        public ConcurrentInitialSetCache(int capacity) {
            Preconditions.checkPositive("capacity", capacity);
            this.capacity = capacity;
            this.cache = new ConcurrentHashMap<>(capacity);
        }

        public V computeIfAbsent(K key, Function<? super K, ? extends V> valueFunction) {
            V value = cache.get(key);
            if (value == null) {
                if (cache.size() < capacity) {
                    // use CHM.computeIfAbsent to avoid duplicate calculation of a single key
                    value = cache.computeIfAbsent(key, valueFunction);
                } else {
                    value = valueFunction.apply(key);
                }
            }
            return value;
        }
    }
}
