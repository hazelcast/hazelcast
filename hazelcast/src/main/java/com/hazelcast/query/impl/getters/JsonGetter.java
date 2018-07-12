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

package com.hazelcast.query.impl.getters;

import com.eclipsesource.json.JsonValue;
import com.hazelcast.core.JsonString;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import java.util.AbstractMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.util.EmptyStatement.ignore;

public class JsonGetter extends Getter {

    private static final Pattern ARRAY_PATTERN = Pattern.compile("(.*)\\[(\\d+)\\]");

    public JsonGetter() {
        super(null);
    }

    public JsonGetter(Getter parent) {
        super(parent);
    }

    @Override
    Object getValue(Object obj) {
        JsonString json = (JsonString) obj;
        return json.asJsonValue();
    }

    @Override
    Object getValue(Object obj, String attributePath) {
        String[] paths = getPath(attributePath);
        JsonValue value = (JsonValue) getValue(obj);
        if (value.isObject()) {
            for (String path : paths) {
                if (value == null) {
                    return null;
                }
                Map.Entry<String, Integer> arr = getPathAndIndexIfArray(path);
                if (arr == null) {
                    value = value.asObject().get(path);
                } else {
                    JsonValue jsonArray = value.asObject().get(arr.getKey());
                    if (jsonArray == null || !jsonArray.isArray()) {
                        return null;
                    }
                    try {
                        value = jsonArray.asArray().get(arr.getValue());
                    } catch (IndexOutOfBoundsException ex) {
                        ignore(ex);
                        return null;
                    }
                }
            }
            return convertFromJsonValue(value);
        }
        return null;
    }

    private Object convertFromJsonValue(JsonValue value) {
        if (value == null) {
            return null;
        } else if (value.isNumber()) {
            return value.asDouble();
        } else if (value.isBoolean()) {
            return value.asBoolean();
        } else if (value.isNull()) {
            return null;
        } else if (value.isString()) {
            return value.asString();
        }
        throw new HazelcastSerializationException("Unknown Json type: " + value);
    }

    @Override
    Class getReturnType() {
        return JsonValue.class;
    }

    @Override
    boolean isCacheable() {
        return true;
    }

    private String[] getPath(String attributePath) {
        return attributePath.split("\\.");
    }

    private Map.Entry<String, Integer> getPathAndIndexIfArray(String path) {
        Matcher matcher = ARRAY_PATTERN.matcher(path);
        if (matcher.matches()) {
            String withoutArg = matcher.group(1);
            int index = Integer.parseInt(matcher.group(2));
            return new AbstractMap.SimpleEntry<String, Integer>(withoutArg, index);
        }
        return null;
    }
}
