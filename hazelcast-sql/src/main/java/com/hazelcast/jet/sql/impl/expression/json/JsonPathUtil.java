/*
 * Copyright 2023 Hazelcast Inc.
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.ConcurrentInitialSetCache;
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
}
