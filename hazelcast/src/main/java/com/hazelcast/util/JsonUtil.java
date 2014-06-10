package com.hazelcast.util;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

/**
 * Utility class to deal with Json.
 */
public final class JsonUtil {

    public static int getInt(JsonObject object, String field) {
        final JsonValue value = object.get(field);
        throwExceptionIfNull(value, field);
        return value.asInt();
    }

    public static int getInt(JsonObject object, String field, int defaultValue) {
        final JsonValue value = object.get(field);
        if (value == null) {
            return defaultValue;
        } else {
            return value.asInt();
        }
    }

    public static long getLong(JsonObject object, String field) {
        final JsonValue value = object.get(field);
        throwExceptionIfNull(value, field);
        return value.asLong();
    }

    public static long getLong(JsonObject object, String field, long defaultValue) {
        final JsonValue value = object.get(field);
        if (value == null) {
            return defaultValue;
        } else {
            return value.asLong();
        }
    }

    public static double getDouble(JsonObject object, String field) {
        final JsonValue value = object.get(field);
        throwExceptionIfNull(value, field);
        return value.asDouble();
    }

    public static double getDouble(JsonObject object, String field, double defaultValue) {
        final JsonValue value = object.get(field);
        if (value == null) {
            return defaultValue;
        } else {
            return value.asDouble();
        }
    }

    public static float getFloat(JsonObject object, String field) {
        final JsonValue value = object.get(field);
        throwExceptionIfNull(value, field);
        return value.asFloat();
    }

    public static float getFloat(JsonObject object, String field, float defaultValue) {
        final JsonValue value = object.get(field);
        if (value == null) {
            return defaultValue;
        } else {
            return value.asFloat();
        }
    }

    public static String getString(JsonObject object, String field) {
        final JsonValue value = object.get(field);
        throwExceptionIfNull(value, field);
        return value.asString();
    }

    public static String getString(JsonObject object, String field, String defaultValue) {
        final JsonValue value = object.get(field);
        if (value == null) {
            return defaultValue;
        } else {
            return value.asString();
        }
    }

    public static boolean getBoolean(JsonObject object, String field) {
        final JsonValue value = object.get(field);
        throwExceptionIfNull(value, field);
        return value.asBoolean();
    }

    public static boolean getBoolean(JsonObject object, String field, boolean defaultValue) {
        final JsonValue value = object.get(field);
        if (value == null) {
            return defaultValue;
        } else {
            return value.asBoolean();
        }
    }

    public static JsonArray getArray(JsonObject object, String field) {
        final JsonValue value = object.get(field);
        throwExceptionIfNull(value, field);
        return value.asArray();
    }

    public static JsonArray getArray(JsonObject object, String field, JsonArray defaultValue) {
        final JsonValue value = object.get(field);
        if (value == null) {
            return defaultValue;
        } else {
            return value.asArray();
        }
    }

    public static JsonObject getObject(JsonObject object, String field) {
        final JsonValue value = object.get(field);
        throwExceptionIfNull(value, field);
        return value.asObject();
    }

    public static JsonObject getObject(JsonObject object, String field, JsonObject defaultValue) {
        final JsonValue value = object.get(field);
        if (value == null) {
            return defaultValue;
        } else {
            return value.asObject();
        }
    }

    private static void throwExceptionIfNull(JsonValue value, String field) {
        if (value == null) {
            throw new IllegalArgumentException("No field found named : " + field);
        }
    }

    private JsonUtil(){}
}
