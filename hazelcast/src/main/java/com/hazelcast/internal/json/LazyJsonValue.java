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

package com.hazelcast.internal.json;

import com.hazelcast.json.JsonArray;
import com.hazelcast.json.JsonObject;
import com.hazelcast.json.JsonValue;

public class LazyJsonValue implements JsonValue {

    private String originalString;
    private JsonValue object;

    public LazyJsonValue(String originalString) {
        this.originalString = originalString;
    }

    @Override
    public boolean isObject() {
        return getObject().isObject();
    }

    @Override
    public boolean isArray() {
        return getObject().isArray();
    }

    @Override
    public boolean isNumber() {
        return getObject().isNumber();
    }

    @Override
    public boolean isString() {
        return getObject().isString();
    }

    @Override
    public boolean isBoolean() {
        return getObject().isBoolean();
    }

    @Override
    public boolean isTrue() {
        return getObject().isTrue();
    }

    @Override
    public boolean isFalse() {
        return getObject().isFalse();
    }

    @Override
    public boolean isNull() {
        return getObject().isNull();
    }

    @Override
    public JsonObject asObject() {
        return getObject().asObject();
    }

    @Override
    public JsonArray asArray() {
        return getObject().asArray();
    }

    @Override
    public int asInt() {
        return getObject().asInt();
    }

    @Override
    public long asLong() {
        return getObject().asLong();
    }

    @Override
    public float asFloat() {
        return getObject().asFloat();
    }

    @Override
    public double asDouble() {
        return getObject().asDouble();
    }

    @Override
    public String asString() {
        return getObject().asString();
    }

    @Override
    public String toString() {
        return getObject().toString();
    }

    @Override
    public boolean asBoolean() {
        return getObject().asBoolean();
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + getObject().hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }

        if (o instanceof com.hazelcast.internal.json.JsonValue) {
            return getObject().equals(o);
        }

        if (o.getClass() != getClass()) {
            return false;
        }

        LazyJsonValue other = (LazyJsonValue)o;

        return getObject().equals(other.getObject());
    }

    public String toSerializationString() {
        if (originalString != null) {
            return originalString;
        }
        return toString();
    }

    protected JsonValue getObject() {
        if (originalString == null) {
            return object;
        }
        object = Json.parse(originalString);
        originalString = null;
        return object;
    }

    // for testing
    boolean isParsed() {
        return object != null;
    }
}
