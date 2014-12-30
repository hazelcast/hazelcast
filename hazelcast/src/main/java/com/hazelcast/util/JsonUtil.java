package com.hazelcast.util;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

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
        if (value == null) {
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
        if (value == null) {
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
        if (value == null) {
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
        if (value == null) {
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
        if (value == null) {
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
        if (value == null) {
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
        if (value == null) {
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
        if (value == null) {
            return defaultValue;
        } else {
            return value.asObject();
        }
    }

    /**
     * Throws IllegalArgumentException if the Json field is not found.
     */
    private static void throwExceptionIfNull(JsonValue value, String field) {
        if (value == null) {
            throw new IllegalArgumentException("No field found named : " + field);
        }
    }

}
