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

/**
 * Represents a JSON value. This can be a JSON <strong>object</strong>, an <strong> array</strong>,
 * a <strong>number</strong>, a <strong>string</strong>, or one of the literals
 * <strong>true</strong>, <strong>false</strong>, and <strong>null</strong>.
 * <p>
 * The literals <strong>true</strong>, <strong>false</strong>, and <strong>null</strong> are
 * represented by the constants {@link #TRUE}, {@link #FALSE}, and {@link #NULL}.
 * </p>
 * <p>
 * JSON <strong>objects</strong> and <strong>arrays</strong> are represented by the subtypes
 * {@link JsonObject} and {@link JsonArray}. Instances of these types can be created using the
 * public constructors of these classes.
 * </p>
 * <p>
 * In order to find out whether an instance of this class is of a certain type, the methods
 * {@link #isObject()}, {@link #isArray()}, {@link #isString()}, {@link #isNumber()} etc. can be
 * used.
 * </p>
 * <p>
 * If the type of a JSON value is known, the methods {@link #asObject()}, {@link #asArray()},
 * {@link #asString()}, {@link #asInt()}, etc. can be used to get this value directly in the
 * appropriate target type.
 * </p>
 * <p>
 * This class is <strong>not supposed to be extended</strong> by clients.
 * </p>
 */
public interface JsonValue {

    /**
     * Detects whether this value represents a JSON object. If this is the case, this value is an
     * instance of {@link JsonObject}.
     *
     * @return <code>true</code> if this value is an instance of JsonObject
     */
    boolean isObject();

    /**
     * Detects whether this value represents a JSON array. If this is the case, this value is an
     * instance of {@link JsonArray}.
     *
     * @return <code>true</code> if this value is an instance of JsonArray
     */
    boolean isArray();

    /**
     * Detects whether this value represents a JSON number.
     *
     * @return <code>true</code> if this value represents a JSON number
     */
    boolean isNumber();

    /**
     * Detects whether this value represents a JSON string.
     *
     * @return <code>true</code> if this value represents a JSON string
     */
    boolean isString();

    /**
     * Detects whether this value represents a boolean value.
     *
     * @return <code>true</code> if this value represents either the JSON literal <code>true</code> or
     * <code>false</code>
     */
    boolean isBoolean();

    /**
     * Detects whether this value represents the JSON literal <code>true</code>.
     *
     * @return <code>true</code> if this value represents the JSON literal <code>true</code>
     */
    boolean isTrue();

    /**
     * Detects whether this value represents the JSON literal <code>false</code>.
     *
     * @return <code>true</code> if this value represents the JSON literal <code>false</code>
     */
    boolean isFalse();

    /**
     * Detects whether this value represents the JSON literal <code>null</code>.
     *
     * @return <code>true</code> if this value represents the JSON literal <code>null</code>
     */
    boolean isNull();

    /**
     * Returns this JSON value as {@link JsonObject}, assuming that this value represents a JSON
     * object. If this is not the case, an exception is thrown.
     *
     * @return a JSONObject for this value
     * @throws UnsupportedOperationException if this value is not a JSON object
     */
    JsonObject asObject();

    /**
     * Returns this JSON value as {@link JsonArray}, assuming that this value represents a JSON array.
     * If this is not the case, an exception is thrown.
     *
     * @return a JSONArray for this value
     * @throws UnsupportedOperationException if this value is not a JSON array
     */
    JsonArray asArray();

    /**
     * Returns this JSON value as an <code>int</code> value, assuming that this value represents a
     * JSON number that can be interpreted as Java <code>int</code>. If this is not the case, an
     * exception is thrown.
     * <p>
     * To be interpreted as Java <code>int</code>, the JSON number must neither contain an exponent
     * nor a fraction part. Moreover, the number must be in the <code>Integer</code> range.
     * </p>
     *
     * @return this value as <code>int</code>
     * @throws UnsupportedOperationException if this value is not a JSON number
     * @throws NumberFormatException         if this JSON number can not be interpreted as <code>int</code> value
     */
    int asInt();

    /**
     * Returns this JSON value as a <code>long</code> value, assuming that this value represents a
     * JSON number that can be interpreted as Java <code>long</code>. If this is not the case, an
     * exception is thrown.
     * <p>
     * To be interpreted as Java <code>long</code>, the JSON number must neither contain an exponent
     * nor a fraction part. Moreover, the number must be in the <code>Long</code> range.
     * </p>
     *
     * @return this value as <code>long</code>
     * @throws UnsupportedOperationException if this value is not a JSON number
     * @throws NumberFormatException         if this JSON number can not be interpreted as <code>long</code> value
     */
    long asLong();

    /**
     * Returns this JSON value as a <code>float</code> value, assuming that this value represents a
     * JSON number. If this is not the case, an exception is thrown.
     * <p>
     * If the JSON number is out of the <code>Float</code> range, {@link Float#POSITIVE_INFINITY} or
     * {@link Float#NEGATIVE_INFINITY} is returned.
     * </p>
     *
     * @return this value as <code>float</code>
     * @throws UnsupportedOperationException if this value is not a JSON number
     */
    float asFloat();

    /**
     * Returns this JSON value as a <code>double</code> value, assuming that this value represents a
     * JSON number. If this is not the case, an exception is thrown.
     * <p>
     * If the JSON number is out of the <code>Double</code> range, {@link Double#POSITIVE_INFINITY} or
     * {@link Double#NEGATIVE_INFINITY} is returned.
     * </p>
     *
     * @return this value as <code>double</code>
     * @throws UnsupportedOperationException if this value is not a JSON number
     */
    double asDouble();

    /**
     * Returns this JSON value as String, assuming that this value represents a JSON string. If this
     * is not the case, an exception is thrown.
     *
     * @return the string represented by this value
     * @throws UnsupportedOperationException if this value is not a JSON string
     */
    String asString();

    /**
     * Returns this JSON value as a <code>boolean</code> value, assuming that this value is either
     * <code>true</code> or <code>false</code>. If this is not the case, an exception is thrown.
     *
     * @return this value as <code>boolean</code>
     * @throws UnsupportedOperationException if this value is neither <code>true</code> or <code>false</code>
     */
    boolean asBoolean();

}
