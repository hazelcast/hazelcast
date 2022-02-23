/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.util;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;

import java.util.Collections;
import java.util.Map;

/**
 * Utility class to deal with Json.
 */
public final class JsonUtil {

    private JsonUtil() {
    }

    /**
     * Returns a field in a Json object as an int.
     * Throws IllegalArgumentException if the field value is null.
     *
     * @param object the Json Object
     * @param field the field in the Json object to return
     * @return the Json field value as an int
     */
    public static int getInt(JsonObject object, String field) {
        final JsonValue value = object.get(field);
        throwExceptionIfNull(value, field);
        return value.asInt();
    }

    /**
     * Returns a field in a Json object as an int.
     *
     * @param object the Json Object
     * @param field the field in the Json object to return
     * @param defaultValue a default value for the field if the field value is null
     * @return the Json field value as an int
     */
    public static int getInt(JsonObject object, String field, int defaultValue) {
        final JsonValue value = object.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        } else {
            return value.asInt();
        }
    }

    /**
     * Returns a field in a Json object as a long.
     * Throws IllegalArgumentException if the field value is null.
     *
     * @param object the Json Object
     * @param field the field in the Json object to return
     * @return the Json field value as a long
     */
    public static long getLong(JsonObject object, String field) {
        final JsonValue value = object.get(field);
        throwExceptionIfNull(value, field);
        return value.asLong();
    }

    /**
     * Returns a field in a Json object as a long.
     *
     * @param object the Json Object
     * @param field the field in the Json object to return
     * @param defaultValue a default value for the field if the field value is null
     * @return the Json field value as a long
     */
    public static long getLong(JsonObject object, String field, long defaultValue) {
        final JsonValue value = object.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        } else {
            return value.asLong();
        }
    }

    /**
     * Returns a field in a Json object as a double.
     * Throws IllegalArgumentException if the field value is null.
     *
     * @param object the Json Object
     * @param field the field in the Json object to return
     * @return the Json field value as a double
     */
    public static double getDouble(JsonObject object, String field) {
        final JsonValue value = object.get(field);
        throwExceptionIfNull(value, field);
        return value.asDouble();
    }
    /**
     * Returns a field in a Json object as a double.
     *
     * @param object the Json Object
     * @param field the field in the Json object to return
     * @param defaultValue a default value for the field if the field value is null
     * @return the Json field value as a double
     */
    public static double getDouble(JsonObject object, String field, double defaultValue) {
        final JsonValue value = object.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        } else {
            return value.asDouble();
        }
    }

    /**
     * Returns a field in a Json object as a float.
     * Throws IllegalArgumentException if the field value is null.
     *
     * @param object the Json Object
     * @param field the field in the Json object to return
     * @return the Json field value as a float
     */
    public static float getFloat(JsonObject object, String field) {
        final JsonValue value = object.get(field);
        throwExceptionIfNull(value, field);
        return value.asFloat();
    }

    /**
     * Returns a field in a Json object as a float.
     *
     * @param object the Json Object
     * @param field the field in the Json object to return
     * @param defaultValue a default value for the field if the field value is null
     * @return the Json field value as a float
     */
    public static float getFloat(JsonObject object, String field, float defaultValue) {
        final JsonValue value = object.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        } else {
            return value.asFloat();
        }
    }

    /**
     * Returns a field in a Json object as a string.
     * Throws IllegalArgumentException if the field value is null.
     *
     * @param object the Json Object
     * @param field the field in the Json object to return
     * @return the Json field value as a string
     */
    public static String getString(JsonObject object, String field) {
        final JsonValue value = object.get(field);
        throwExceptionIfNull(value, field);
        return value.asString();
    }

    /**
     * Returns a field in a Json object as a string.
     *
     * @param object the Json Object
     * @param field the field in the Json object to return
     * @param defaultValue a default value for the field if the field value is null
     * @return the Json field value as a string
     */
    public static String getString(JsonObject object, String field, String defaultValue) {
        final JsonValue value = object.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        } else {
            return value.asString();
        }
    }

    /**
     * Returns a field in a Json object as a boolean.
     * Throws IllegalArgumentException if the field value is null.
     *
     * @param object the Json Object
     * @param field the field in the Json object to return
     * @return the Json field value as a boolean
     */
    public static boolean getBoolean(JsonObject object, String field) {
        final JsonValue value = object.get(field);
        throwExceptionIfNull(value, field);
        return value.asBoolean();
    }

    /**
     * Returns a field in a Json object as a boolean.
     *
     * @param object the Json Object
     * @param field the field in the Json object to return
     * @param defaultValue a default value for the field if the field value is null
     * @return the Json field value as a boolean
     */
    public static boolean getBoolean(JsonObject object, String field, boolean defaultValue) {
        final JsonValue value = object.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        } else {
            return value.asBoolean();
        }
    }

    /**
     * Returns a field in a Json object as an array.
     * Throws IllegalArgumentException if the field value is null.
     *
     * @param object the Json Object
     * @param field the field in the Json object to return
     * @return the Json field value as an array
     */
    public static JsonArray getArray(JsonObject object, String field) {
        final JsonValue value = object.get(field);
        throwExceptionIfNull(value, field);
        return value.asArray();
    }

    /**
     * Returns a field in a Json object as an array.
     *
     * @param object the Json Object
     * @param field the field in the Json object to return
     * @param defaultValue a default value for the field if the field value is null
     * @return the Json field value as a Json array
     */
    public static JsonArray getArray(JsonObject object, String field, JsonArray defaultValue) {
        final JsonValue value = object.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        } else {
            return value.asArray();
        }
    }

    /**
     * Returns a field in a Json object as an object.
     * Throws IllegalArgumentException if the field value is null.
     *
     * @param object the Json object
     * @param field the field in the Json object to return
     * @return the Json field value as a Json object
     */
    public static JsonObject getObject(JsonObject object, String field) {
        final JsonValue value = object.get(field);
        throwExceptionIfNull(value, field);
        return value.asObject();
    }

    /**
     * Returns a field in a Json object as an object.
     *
     * @param object the Json object
     * @param field the field in the Json object to return
     * @param defaultValue a default value for the field if the field value is null
     * @return the Json field value as a Json object
     */
    public static JsonObject getObject(JsonObject object, String field, JsonObject defaultValue) {
        final JsonValue value = object.get(field);
        if (value == null || value.isNull()) {
            return defaultValue;
        } else {
            return value.asObject();
        }
    }

    /**
     * Transforms the provided {@link JsonObject} int a map of name/value pairs.
     *
     * @param object the JSON object
     * @return map from JSON name to value
     */
    public static Map<String, Comparable> fromJsonObject(JsonObject object) {
        if (object == null || object.isNull() || object.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Comparable> map = MapUtil.createHashMap(object.size());
        for (String propertyName : object.names()) {
            map.put(propertyName, object.get(propertyName).asString());
        }
        return map;
    }

    /**
     * Transforms the provided map of name/value pairs into a {@link JsonObject}.
     *
     * @param map map of JSON name-value pairs
     * @return the JSON object
     */
    public static JsonObject toJsonObject(Map<String, ?> map) {
        JsonObject properties = new JsonObject();
        for (Map.Entry<String, ?> property : map.entrySet()) {
            properties.add(property.getKey(), Json.value(property.getValue().toString()));
        }
        return properties;
    }

    /**
     * Throws IllegalArgumentException if the Json field is not found.
     */
    private static void throwExceptionIfNull(JsonValue value, String field) {
        if (value == null) {
            throw new IllegalArgumentException("No field found: " + field);
        }
    }
}
