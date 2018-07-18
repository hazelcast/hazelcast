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
import java.io.Writer;
import java.util.List;

/**
 * Represents a JSON object, a set of name/value pairs, where the names are strings and the values
 * are JSON values.
 * <p>
 * Members can be added using the <code>add(String, ...)</code> methods which accept instances of
 * {@link JsonValue}, strings, primitive numbers, and boolean values. To modify certain values of an
 * object, use the <code>set(String, ...)</code> methods. Please note that the <code>add</code>
 * methods are faster than <code>set</code> as they do not search for existing members. On the other
 * hand, the <code>add</code> methods do not prevent adding multiple members with the same name.
 * Duplicate names are discouraged but not prohibited by JSON.
 * </p>
 * <p>
 * Members can be accessed by their name using {@link #get(String)}. A list of all names can be
 * obtained from the method {@link #names()}. This class also supports iterating over the members in
 * document order using an {@link #iterator()} or an enhanced for loop:
 * </p>
 * <pre>
 * for (Member member : jsonObject) {
 *   String name = member.getName();
 *   JsonValue value = member.getValue();
 *   ...
 * }
 * </pre>
 * <p>
 * Even though JSON objects are unordered by definition, instances of this class preserve the order
 * of members to allow processing in document order and to guarantee a predictable output.
 * </p>
 * <p>
 * Note that this class is <strong>not thread-safe</strong>. If multiple threads access a
 * <code>JsonObject</code> instance concurrently, while at least one of these threads modifies the
 * contents of this object, access to the instance must be synchronized externally. Failure to do so
 * may lead to an inconsistent state.
 * </p>
 * <p>
 * This class is <strong>not supposed to be extended</strong> by clients.
 * </p>
 */
public interface JsonObject extends JsonValue, Iterable<JsonObjectEntry> {

    /**
     * Appends a new member to the end of this object, with the specified name and the JSON
     * representation of the specified <code>int</code> value.
     * <p>
     * This method <strong>does not prevent duplicate names</strong>. Calling this method with a name
     * that already exists in the object will append another member with the same name. In order to
     * replace existing members, use the method <code>set(name, value)</code> instead. However,
     * <strong> <em>add</em> is much faster than <em>set</em></strong> (because it does not need to
     * search for existing members). Therefore <em>add</em> should be preferred when constructing new
     * objects.
     * </p>
     *
     * @param name  the name of the member to add
     * @param value the value of the member to add
     * @return the object itself, to enable method chaining
     */
    JsonObject add(String name, int value);

    /**
     * Appends a new member to the end of this object, with the specified name and the JSON
     * representation of the specified <code>long</code> value.
     * <p>
     * This method <strong>does not prevent duplicate names</strong>. Calling this method with a name
     * that already exists in the object will append another member with the same name. In order to
     * replace existing members, use the method <code>set(name, value)</code> instead. However,
     * <strong> <em>add</em> is much faster than <em>set</em></strong> (because it does not need to
     * search for existing members). Therefore <em>add</em> should be preferred when constructing new
     * objects.
     * </p>
     *
     * @param name  the name of the member to add
     * @param value the value of the member to add
     * @return the object itself, to enable method chaining
     */
    JsonObject add(String name, long value);

    /**
     * Appends a new member to the end of this object, with the specified name and the JSON
     * representation of the specified <code>float</code> value.
     * <p>
     * This method <strong>does not prevent duplicate names</strong>. Calling this method with a name
     * that already exists in the object will append another member with the same name. In order to
     * replace existing members, use the method <code>set(name, value)</code> instead. However,
     * <strong> <em>add</em> is much faster than <em>set</em></strong> (because it does not need to
     * search for existing members). Therefore <em>add</em> should be preferred when constructing new
     * objects.
     * </p>
     *
     * @param name  the name of the member to add
     * @param value the value of the member to add
     * @return the object itself, to enable method chaining
     */
    JsonObject add(String name, float value);

    /**
     * Appends a new member to the end of this object, with the specified name and the JSON
     * representation of the specified <code>double</code> value.
     * <p>
     * This method <strong>does not prevent duplicate names</strong>. Calling this method with a name
     * that already exists in the object will append another member with the same name. In order to
     * replace existing members, use the method <code>set(name, value)</code> instead. However,
     * <strong> <em>add</em> is much faster than <em>set</em></strong> (because it does not need to
     * search for existing members). Therefore <em>add</em> should be preferred when constructing new
     * objects.
     * </p>
     *
     * @param name  the name of the member to add
     * @param value the value of the member to add
     * @return the object itself, to enable method chaining
     */
    JsonObject add(String name, double value);

    /**
     * Appends a new member to the end of this object, with the specified name and the JSON
     * representation of the specified <code>boolean</code> value.
     * <p>
     * This method <strong>does not prevent duplicate names</strong>. Calling this method with a name
     * that already exists in the object will append another member with the same name. In order to
     * replace existing members, use the method <code>set(name, value)</code> instead. However,
     * <strong> <em>add</em> is much faster than <em>set</em></strong> (because it does not need to
     * search for existing members). Therefore <em>add</em> should be preferred when constructing new
     * objects.
     * </p>
     *
     * @param name  the name of the member to add
     * @param value the value of the member to add
     * @return the object itself, to enable method chaining
     */
    JsonObject add(String name, boolean value);

    /**
     * Appends a new member to the end of this object, with the specified name and the JSON
     * representation of the specified string.
     * <p>
     * This method <strong>does not prevent duplicate names</strong>. Calling this method with a name
     * that already exists in the object will append another member with the same name. In order to
     * replace existing members, use the method <code>set(name, value)</code> instead. However,
     * <strong> <em>add</em> is much faster than <em>set</em></strong> (because it does not need to
     * search for existing members). Therefore <em>add</em> should be preferred when constructing new
     * objects.
     * </p>
     *
     * @param name  the name of the member to add
     * @param value the value of the member to add
     * @return the object itself, to enable method chaining
     */
    JsonObject add(String name, String value);

    /**
     * Appends a new member to the end of this object, with the specified name and the specified JSON
     * value.
     * <p>
     * This method <strong>does not prevent duplicate names</strong>. Calling this method with a name
     * that already exists in the object will append another member with the same name. In order to
     * replace existing members, use the method <code>set(name, value)</code> instead. However,
     * <strong> <em>add</em> is much faster than <em>set</em></strong> (because it does not need to
     * search for existing members). Therefore <em>add</em> should be preferred when constructing new
     * objects.
     * </p>
     *
     * @param name  the name of the member to add
     * @param value the value of the member to add, must not be <code>null</code>
     * @return the object itself, to enable method chaining
     */
    JsonObject add(String name, JsonValue value);

    /**
     * Sets the value of the member with the specified name to the JSON representation of the
     * specified <code>int</code> value. If this object does not contain a member with this name, a
     * new member is added at the end of the object. If this object contains multiple members with
     * this name, only the last one is changed.
     * <p>
     * This method should <strong>only be used to modify existing objects</strong>. To fill a new
     * object with members, the method <code>add(name, value)</code> should be preferred which is much
     * faster (as it does not need to search for existing members).
     * </p>
     *
     * @param name  the name of the member to replace
     * @param value the value to set to the member
     * @return the object itself, to enable method chaining
     */
    JsonObject set(String name, int value);

    /**
     * Sets the value of the member with the specified name to the JSON representation of the
     * specified <code>long</code> value. If this object does not contain a member with this name, a
     * new member is added at the end of the object. If this object contains multiple members with
     * this name, only the last one is changed.
     * <p>
     * This method should <strong>only be used to modify existing objects</strong>. To fill a new
     * object with members, the method <code>add(name, value)</code> should be preferred which is much
     * faster (as it does not need to search for existing members).
     * </p>
     *
     * @param name  the name of the member to replace
     * @param value the value to set to the member
     * @return the object itself, to enable method chaining
     */
    JsonObject set(String name, long value);

    /**
     * Sets the value of the member with the specified name to the JSON representation of the
     * specified <code>float</code> value. If this object does not contain a member with this name, a
     * new member is added at the end of the object. If this object contains multiple members with
     * this name, only the last one is changed.
     * <p>
     * This method should <strong>only be used to modify existing objects</strong>. To fill a new
     * object with members, the method <code>add(name, value)</code> should be preferred which is much
     * faster (as it does not need to search for existing members).
     * </p>
     *
     * @param name  the name of the member to add
     * @param value the value of the member to add
     * @return the object itself, to enable method chaining
     */
    JsonObject set(String name, float value);

    /**
     * Sets the value of the member with the specified name to the JSON representation of the
     * specified <code>double</code> value. If this object does not contain a member with this name, a
     * new member is added at the end of the object. If this object contains multiple members with
     * this name, only the last one is changed.
     * <p>
     * This method should <strong>only be used to modify existing objects</strong>. To fill a new
     * object with members, the method <code>add(name, value)</code> should be preferred which is much
     * faster (as it does not need to search for existing members).
     * </p>
     *
     * @param name  the name of the member to add
     * @param value the value of the member to add
     * @return the object itself, to enable method chaining
     */
    JsonObject set(String name, double value);

    /**
     * Sets the value of the member with the specified name to the JSON representation of the
     * specified <code>boolean</code> value. If this object does not contain a member with this name,
     * a new member is added at the end of the object. If this object contains multiple members with
     * this name, only the last one is changed.
     * <p>
     * This method should <strong>only be used to modify existing objects</strong>. To fill a new
     * object with members, the method <code>add(name, value)</code> should be preferred which is much
     * faster (as it does not need to search for existing members).
     * </p>
     *
     * @param name  the name of the member to add
     * @param value the value of the member to add
     * @return the object itself, to enable method chaining
     */
    JsonObject set(String name, boolean value);

    /**
     * Sets the value of the member with the specified name to the JSON representation of the
     * specified string. If this object does not contain a member with this name, a new member is
     * added at the end of the object. If this object contains multiple members with this name, only
     * the last one is changed.
     * <p>
     * This method should <strong>only be used to modify existing objects</strong>. To fill a new
     * object with members, the method <code>add(name, value)</code> should be preferred which is much
     * faster (as it does not need to search for existing members).
     * </p>
     *
     * @param name  the name of the member to add
     * @param value the value of the member to add
     * @return the object itself, to enable method chaining
     */
    JsonObject set(String name, String value);

    /**
     * Sets the value of the member with the specified name to the specified JSON value. If this
     * object does not contain a member with this name, a new member is added at the end of the
     * object. If this object contains multiple members with this name, only the last one is changed.
     * <p>
     * This method should <strong>only be used to modify existing objects</strong>. To fill a new
     * object with members, the method <code>add(name, value)</code> should be preferred which is much
     * faster (as it does not need to search for existing members).
     * </p>
     *
     * @param name  the name of the member to add
     * @param value the value of the member to add, must not be <code>null</code>
     * @return the object itself, to enable method chaining
     */
    JsonObject set(String name, JsonValue value);

    /**
     * Removes a member with the specified name from this object. If this object contains multiple
     * members with the given name, only the last one is removed. If this object does not contain a
     * member with the specified name, the object is not modified.
     *
     * @param name the name of the member to remove
     * @return the object itself, to enable method chaining
     */
    JsonObject remove(String name);

    /**
     * Copies all members of the specified object into this object. When the specified object contains
     * members with names that also exist in this object, the existing values in this object will be
     * replaced by the corresponding values in the specified object.
     *
     * @param object the object to merge
     * @return the object itself, to enable method chaining
     */
    JsonObject merge(JsonObject object);

    /**
     * Returns the value of the member with the specified name in this object. If this object contains
     * multiple members with the given name, this method will return the last one.
     *
     * @param name the name of the member whose value is to be returned
     * @return the value of the last member with the specified name, or <code>null</code> if this
     * object does not contain a member with that name
     */
    JsonValue get(String name);

    /**
     * Returns the <code>int</code> value of the member with the specified name in this object. If
     * this object does not contain a member with this name, the given default value is returned. If
     * this object contains multiple members with the given name, the last one will be picked. If this
     * member's value does not represent a JSON number or if it cannot be interpreted as Java
     * <code>int</code>, an exception is thrown.
     *
     * @param name         the name of the member whose value is to be returned
     * @param defaultValue the value to be returned if the requested member is missing
     * @return the value of the last member with the specified name, or the given default value if
     * this object does not contain a member with that name
     */
    int getInt(String name, int defaultValue);

    /**
     * Returns the <code>long</code> value of the member with the specified name in this object. If
     * this object does not contain a member with this name, the given default value is returned. If
     * this object contains multiple members with the given name, the last one will be picked. If this
     * member's value does not represent a JSON number or if it cannot be interpreted as Java
     * <code>long</code>, an exception is thrown.
     *
     * @param name         the name of the member whose value is to be returned
     * @param defaultValue the value to be returned if the requested member is missing
     * @return the value of the last member with the specified name, or the given default value if
     * this object does not contain a member with that name
     */
    long getLong(String name, long defaultValue);

    /**
     * Returns the <code>float</code> value of the member with the specified name in this object. If
     * this object does not contain a member with this name, the given default value is returned. If
     * this object contains multiple members with the given name, the last one will be picked. If this
     * member's value does not represent a JSON number or if it cannot be interpreted as Java
     * <code>float</code>, an exception is thrown.
     *
     * @param name         the name of the member whose value is to be returned
     * @param defaultValue the value to be returned if the requested member is missing
     * @return the value of the last member with the specified name, or the given default value if
     * this object does not contain a member with that name
     */
    float getFloat(String name, float defaultValue);

    /**
     * Returns the <code>double</code> value of the member with the specified name in this object. If
     * this object does not contain a member with this name, the given default value is returned. If
     * this object contains multiple members with the given name, the last one will be picked. If this
     * member's value does not represent a JSON number or if it cannot be interpreted as Java
     * <code>double</code>, an exception is thrown.
     *
     * @param name         the name of the member whose value is to be returned
     * @param defaultValue the value to be returned if the requested member is missing
     * @return the value of the last member with the specified name, or the given default value if
     * this object does not contain a member with that name
     */
    double getDouble(String name, double defaultValue);

    /**
     * Returns the <code>boolean</code> value of the member with the specified name in this object. If
     * this object does not contain a member with this name, the given default value is returned. If
     * this object contains multiple members with the given name, the last one will be picked. If this
     * member's value does not represent a JSON <code>true</code> or <code>false</code> value, an
     * exception is thrown.
     *
     * @param name         the name of the member whose value is to be returned
     * @param defaultValue the value to be returned if the requested member is missing
     * @return the value of the last member with the specified name, or the given default value if
     * this object does not contain a member with that name
     */
    boolean getBoolean(String name, boolean defaultValue);

    /**
     * Returns the <code>String</code> value of the member with the specified name in this object. If
     * this object does not contain a member with this name, the given default value is returned. If
     * this object contains multiple members with the given name, the last one is picked. If this
     * member's value does not represent a JSON string, an exception is thrown.
     *
     * @param name         the name of the member whose value is to be returned
     * @param defaultValue the value to be returned if the requested member is missing
     * @return the value of the last member with the specified name, or the given default value if
     * this object does not contain a member with that name
     */
    String getString(String name, String defaultValue);

    /**
     * Returns the number of members (name/value pairs) in this object.
     *
     * @return the number of members in this object
     */
    int size();

    /**
     * Returns <code>true</code> if this object contains no members.
     *
     * @return <code>true</code> if this object contains no members
     */
    boolean isEmpty();

    /**
     * Returns a list of the names in this object in document order. The returned list is backed by
     * this object and will reflect subsequent changes. It cannot be used to modify this object.
     * Attempts to modify the returned list will result in an exception.
     *
     * @return a list of the names in this object
     */
    List<String> names();

    void writeTo(Writer writer) throws IOException;

}
