/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.json.internal;

/**
 * A node that describes either a name-value pair in a Json object or
 * an item in a Json array. For arrays items, {@link #getNameStart()}
 * always return -1 whereas for name-value pairs, it represents the
 * location of name of the attribute for object attributes.
 */
public class JsonSchemaNameValue {

    private final int nameStart;
    private final JsonSchemaNode value;

    public JsonSchemaNameValue(int nameStart, JsonSchemaNode value) {
        this.nameStart = nameStart;
        this.value = value;
    }

    /**
     * Points to name of the object described in {@link #getValue()}.
     * The returned integer represents the offset of the name according
     * to the underlying data format. It could be byte offset for Data
     * or char offset for String
     *
     * @return  the location of the name relative to the beginning of the object
     *          -1 for array items
     */
    public int getNameStart() {
        return nameStart;
    }

    /**
     *
     * @return true if this represents an array item
     */
    public boolean isArrayItem() {
        return nameStart == -1;
    }

    /**
     *
     * @return true if this represents an object attribute
     */
    public boolean isObjectItem() {
        return nameStart > 0;
    }

    /**
     * Returns the description of the value stored in here
     * @return
     */
    public JsonSchemaNode getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JsonSchemaNameValue that = (JsonSchemaNameValue) o;

        if (nameStart != that.nameStart) {
            return false;
        }
        return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        int result = nameStart;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "JsonSchemaNameValue{"
                + "nameStart=" + nameStart
                + ", value=" + value
                + '}';
    }
}
