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

package com.hazelcast.core;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonValue;

/**
 * Represents a Json string. Two json strings are equal only if the two string repsentations
 * are exactly the same.
 */
public class JsonStringImpl implements JsonString {

    private JsonValue jsonValue;
    private String original;

    public JsonStringImpl(String json) {
        this.original = json;
    }

    public JsonStringImpl(JsonValue jsonValue) {
        this.jsonValue = jsonValue;
    }

    @Override
    public JsonValue asJsonValue() {
        if (jsonValue == null) {
            jsonValue = Json.parse(original);
        }
        return jsonValue;
    }

    @Override
    public String asString() {
        if (original == null) {
            original = jsonValue.toString();
        }
        return original;
    }

    @Override
    public void set(String str) {
        this.jsonValue = null;
        this.original = str;
    }

    @Override
    public void set(JsonValue value) {
        this.original = null;
        this.jsonValue = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JsonString that = (JsonString) o;
        return that.asString().equals(asString());
    }

    @Override
    public int hashCode() {
        return asString().hashCode();
    }
}
