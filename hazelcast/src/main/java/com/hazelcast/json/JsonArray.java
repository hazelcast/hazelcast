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

import java.util.List;

/**
 * Represents a JSON array, an ordered collection of JSON values.
 * <p>
 * Elements can be added using the <code>add(...)</code> methods which accept instances of
 * {@link JsonValue}, strings, primitive numbers, and boolean values. To replace an element of an
 * array, use the <code>set(int, ...)</code> methods.
 * </p>
 * <p>
 * Elements can be accessed by their index using {@link #get(int)}. This class also supports
 * iterating over the elements in document order using an {@link #iterator()} or an enhanced for
 * loop:
 * </p>
 * <pre>
 * for (JsonValue value : jsonArray) {
 *   ...
 * }
 * </pre>
 * <p>
 * An equivalent {@link List} can be obtained from the method {@link #values()}.
 * </p>
 * <p>
 * Note that this class is <strong>not thread-safe</strong>. If multiple threads access a
 * <code>JsonArray</code> instance concurrently, while at least one of these threads modifies the
 * contents of this array, access to the instance must be synchronized externally. Failure to do so
 * may lead to an inconsistent state.
 * </p>
 * <p>
 * This class is <strong>not supposed to be extended</strong> by clients.
 * </p>
 */
public interface JsonArray extends JsonValue, Iterable<JsonValue> {

    /**
     * Appends the JSON representation of the specified <code>int</code> value to the end of this
     * array.
     *
     * @param value the value to add to the array
     * @return the array itself, to enable method chaining
     */
    JsonArray add(int value);

    /**
     * Appends the JSON representation of the specified <code>long</code> value to the end of this
     * array.
     *
     * @param value the value to add to the array
     * @return the array itself, to enable method chaining
     */
    JsonArray add(long value);

    /**
     * Appends the JSON representation of the specified <code>float</code> value to the end of this
     * array.
     *
     * @param value the value to add to the array
     * @return the array itself, to enable method chaining
     */
    JsonArray add(float value);

    /**
     * Appends the JSON representation of the specified <code>double</code> value to the end of this
     * array.
     *
     * @param value the value to add to the array
     * @return the array itself, to enable method chaining
     */
    JsonArray add(double value);

    /**
     * Appends the JSON representation of the specified <code>boolean</code> value to the end of this
     * array.
     *
     * @param value the value to add to the array
     * @return the array itself, to enable method chaining
     */
    JsonArray add(boolean value);

    /**
     * Appends the JSON representation of the specified string to the end of this array.
     *
     * @param value the string to add to the array
     * @return the array itself, to enable method chaining
     */
    JsonArray add(String value);

    /**
     * Appends the specified JSON value to the end of this array.
     *
     * @param value the JsonValue to add to the array, must not be <code>null</code>
     * @return the array itself, to enable method chaining
     */
    JsonArray add(JsonValue value);

    /**
     * Replaces the element at the specified position in this array with the JSON representation of
     * the specified <code>int</code> value.
     *
     * @param index the index of the array element to replace
     * @param value the value to be stored at the specified array position
     * @return the array itself, to enable method chaining
     * @throws IndexOutOfBoundsException if the index is out of range, i.e. <code>index &lt; 0</code> or
     *                                   <code>index &gt;= size</code>
     */
    JsonArray set(int index, int value);

    /**
     * Replaces the element at the specified position in this array with the JSON representation of
     * the specified <code>long</code> value.
     *
     * @param index the index of the array element to replace
     * @param value the value to be stored at the specified array position
     * @return the array itself, to enable method chaining
     * @throws IndexOutOfBoundsException if the index is out of range, i.e. <code>index &lt; 0</code> or
     *                                   <code>index &gt;= size</code>
     */
    JsonArray set(int index, long value);

    /**
     * Replaces the element at the specified position in this array with the JSON representation of
     * the specified <code>float</code> value.
     *
     * @param index the index of the array element to replace
     * @param value the value to be stored at the specified array position
     * @return the array itself, to enable method chaining
     * @throws IndexOutOfBoundsException if the index is out of range, i.e. <code>index &lt; 0</code> or
     *                                   <code>index &gt;= size</code>
     */
    JsonArray set(int index, float value);

    /**
     * Replaces the element at the specified position in this array with the JSON representation of
     * the specified <code>double</code> value.
     *
     * @param index the index of the array element to replace
     * @param value the value to be stored at the specified array position
     * @return the array itself, to enable method chaining
     * @throws IndexOutOfBoundsException if the index is out of range, i.e. <code>index &lt; 0</code> or
     *                                   <code>index &gt;= size</code>
     */
    JsonArray set(int index, double value);

    /**
     * Replaces the element at the specified position in this array with the JSON representation of
     * the specified <code>boolean</code> value.
     *
     * @param index the index of the array element to replace
     * @param value the value to be stored at the specified array position
     * @return the array itself, to enable method chaining
     * @throws IndexOutOfBoundsException if the index is out of range, i.e. <code>index &lt; 0</code> or
     *                                   <code>index &gt;= size</code>
     */
    JsonArray set(int index, boolean value);

    /**
     * Replaces the element at the specified position in this array with the JSON representation of
     * the specified string.
     *
     * @param index the index of the array element to replace
     * @param value the string to be stored at the specified array position
     * @return the array itself, to enable method chaining
     * @throws IndexOutOfBoundsException if the index is out of range, i.e. <code>index &lt; 0</code> or
     *                                   <code>index &gt;= size</code>
     */
    JsonArray set(int index, String value);

    /**
     * Replaces the element at the specified position in this array with the specified JSON value.
     *
     * @param index the index of the array element to replace
     * @param value the value to be stored at the specified array position, must not be <code>null</code>
     * @return the array itself, to enable method chaining
     * @throws IndexOutOfBoundsException if the index is out of range, i.e. <code>index &lt; 0</code> or
     *                                   <code>index &gt;= size</code>
     */
    JsonArray set(int index, JsonValue value);

    /**
     * Removes the element at the specified index from this array.
     *
     * @param index the index of the element to remove
     * @return the array itself, to enable method chaining
     * @throws IndexOutOfBoundsException if the index is out of range, i.e. <code>index &lt; 0</code> or
     *                                   <code>index &gt;= size</code>
     */
    JsonArray remove(int index);

    /**
     * Returns the number of elements in this array.
     *
     * @return the number of elements in this array
     */
    int size();

    /**
     * Returns <code>true</code> if this array contains no elements.
     *
     * @return <code>true</code> if this array contains no elements
     */
    boolean isEmpty();

    /**
     * Returns the value of the element at the specified position in this array.
     *
     * @param index the index of the array element to return
     * @return the value of the element at the specified position
     * @throws IndexOutOfBoundsException if the index is out of range, i.e. <code>index &lt; 0</code> or
     *                                   <code>index &gt;= size</code>
     */
    JsonValue get(int index);

    /**
     * Returns a list of the values in this array in document order. The returned list is backed by
     * this array and will reflect subsequent changes. It cannot be used to modify this array.
     * Attempts to modify the returned list will result in an exception.
     *
     * @return a list of the values in this array
     */
    List<JsonValue> values();


}
