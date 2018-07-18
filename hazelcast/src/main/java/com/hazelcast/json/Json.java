/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.json;


import java.io.IOException;
import java.io.Reader;

/**
 * This class serves as the entry point to the minimal-json API.
 * <p>
 * To <strong>parse</strong> a given JSON input, use the <code>parse()</code> methods like in this
 * example:
 * </p>
 * <pre>
 * JsonObject object = Json.parse(string).asObject();
 * </pre>
 * <p>
 * To <strong>create</strong> a JSON data structure to be serialized, use the methods
 * <code>value()</code>, <code>array()</code>, and <code>object()</code>. For example, the following
 * snippet will produce the JSON string <em>{"foo": 23, "bar": true}</em>:
 * </p>
 * <pre>
 * String string = Json.object().add("foo", 23).add("bar", true).toString();
 * </pre>
 * <p>
 * To create a JSON array from a given Java array, you can use one of the <code>array()</code>
 * methods with varargs parameters:
 * </p>
 * <pre>
 * String[] names = ...
 * JsonArray array = Json.array(names);
 * </pre>
 */
public final class Json {

    /**
     * Represents the JSON literal <code>null</code>.
     */
    public static final JsonValue NULL = com.hazelcast.internal.json.Json.NULL;

    /**
     * Represents the JSON literal <code>true</code>.
     */
    public static final JsonValue TRUE = com.hazelcast.internal.json.Json.TRUE;

    /**
     * Represents the JSON literal <code>false</code>.
     */
    public static final JsonValue FALSE = com.hazelcast.internal.json.Json.FALSE;

    private Json() {
        // not meant to be instantiated
    }

    /**
     * Returns a JsonValue instance that represents the given <code>int</code> value.
     *
     * @param value the value to get a JSON representation for
     * @return a JSON value that represents the given value
     */
    public static JsonValue value(int value) {
        return com.hazelcast.internal.json.Json.value(value);
    }

    /**
     * Returns a JsonValue instance that represents the given <code>long</code> value.
     *
     * @param value the value to get a JSON representation for
     * @return a JSON value that represents the given value
     */
    public static JsonValue value(long value) {
        return com.hazelcast.internal.json.Json.value(value);
    }

    /**
     * Returns a JsonValue instance that represents the given <code>float</code> value.
     *
     * @param value the value to get a JSON representation for
     * @return a JSON value that represents the given value
     */
    public static JsonValue value(float value) {
        return com.hazelcast.internal.json.Json.value(value);
    }

    /**
     * Returns a JsonValue instance that represents the given <code>double</code> value.
     *
     * @param value the value to get a JSON representation for
     * @return a JSON value that represents the given value
     */
    public static JsonValue value(double value) {
        return com.hazelcast.internal.json.Json.value(value);
    }

    /**
     * Returns a JsonValue instance that represents the given string.
     *
     * @param string the string to get a JSON representation for
     * @return a JSON value that represents the given string
     */
    public static JsonValue value(String string) {
        return com.hazelcast.internal.json.Json.value(string);
    }

    /**
     * Returns a JsonValue instance that represents the given <code>boolean</code> value.
     *
     * @param value the value to get a JSON representation for
     * @return a JSON value that represents the given value
     */
    public static JsonValue value(boolean value) {
        return com.hazelcast.internal.json.Json.value(value);
    }

    /**
     * Creates a new empty JsonArray. This is equivalent to creating a new JsonArray using the
     * constructor.
     *
     * @return a new empty JSON array
     */
    public static JsonArray array() {
        return com.hazelcast.internal.json.Json.array();
    }

    /**
     * Creates a new JsonArray that contains the JSON representations of the given <code>int</code>
     * values.
     *
     * @param values the values to be included in the new JSON array
     * @return a new JSON array that contains the given values
     */
    public static JsonArray array(int... values) {
        return com.hazelcast.internal.json.Json.array(values);
    }

    /**
     * Creates a new JsonArray that contains the JSON representations of the given <code>long</code>
     * values.
     *
     * @param values the values to be included in the new JSON array
     * @return a new JSON array that contains the given values
     */
    public static JsonArray array(long... values) {
        return com.hazelcast.internal.json.Json.array(values);
    }

    /**
     * Creates a new JsonArray that contains the JSON representations of the given <code>float</code>
     * values.
     *
     * @param values the values to be included in the new JSON array
     * @return a new JSON array that contains the given values
     */
    public static JsonArray array(float... values) {
        return com.hazelcast.internal.json.Json.array(values);
    }

    /**
     * Creates a new JsonArray that contains the JSON representations of the given <code>double</code>
     * values.
     *
     * @param values the values to be included in the new JSON array
     * @return a new JSON array that contains the given values
     */
    public static JsonArray array(double... values) {
        return com.hazelcast.internal.json.Json.array(values);
    }

    /**
     * Creates a new JsonArray that contains the JSON representations of the given
     * <code>boolean</code> values.
     *
     * @param values the values to be included in the new JSON array
     * @return a new JSON array that contains the given values
     */
    public static JsonArray array(boolean... values) {
        return com.hazelcast.internal.json.Json.array(values);
    }

    /**
     * Creates a new JsonArray that contains the JSON representations of the given strings.
     *
     * @param strings the strings to be included in the new JSON array
     * @return a new JSON array that contains the given strings
     */
    public static JsonArray array(String... strings) {
        return com.hazelcast.internal.json.Json.array(strings);
    }

    /**
     * Creates a new empty JsonObject. This is equivalent to creating a new JsonObject using the
     * constructor.
     *
     * @return a new empty JSON object
     */
    public static JsonObject object() {
        return com.hazelcast.internal.json.Json.object();
    }

    /**
     * Parses the given input string as JSON. The input must contain a valid JSON value, optionally
     * padded with whitespace.
     *
     * @param string the input string, must be valid JSON
     * @return a value that represents the parsed JSON
     * @throws IllegalArgumentException if the input is not valid JSON
     */
    public static JsonValue parse(String string) {
        return com.hazelcast.internal.json.Json.parse(string);
    }

    public static JsonValue parse(Reader reader) throws IOException {
        return com.hazelcast.internal.json.Json.parse(reader);
    }
}
