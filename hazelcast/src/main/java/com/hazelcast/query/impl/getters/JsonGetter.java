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

import com.hazelcast.json.JsonArray;
import com.hazelcast.json.JsonValue;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.util.EmptyStatement.ignore;

public class JsonGetter extends Getter {

    private static final Pattern PATTERN = Pattern.compile("([^.\\[\\]]+)|(\\[(\\d+|any)\\])");

    public JsonGetter() {
        super(null);
    }

    public JsonGetter(Getter parent) {
        super(parent);
    }

    @Override
    Object getValue(Object obj) {
        return convertFromJsonValue((JsonValue) obj);
    }

    @Override
    Object getValue(Object obj, String attributePath) {
        List<String> paths = getPath(attributePath);
        JsonValue value = (JsonValue) obj;
        if (value.isObject()) {
            for (int i = 0; i < paths.size(); i++) {
                String path = paths.get(i);
                if (value == null) {
                    return null;
                }
                try {
                    if (path.startsWith("[")) {
                        if (!value.isArray()) {
                            return null;
                        }
                        JsonArray valueArray = value.asArray();
                        String indexText = path.substring(1, path.length() - 1);
                        if (indexText.equals("any")) {
                            return getMultiValue(valueArray, paths, i + 1);

                        } else {
                            int index = Integer.parseInt(indexText);
                            value = valueArray.get(index);
                        }
                    } else {
                        value = value.asObject().get(path);
                    }
                } catch (IndexOutOfBoundsException ex) {
                    ignore(ex);
                    return null;
                }
            }
            return convertFromJsonValue(value);
        }
        return null;
    }

    MultiResult getMultiValue(JsonArray arr, List<String> paths, int index) {
        MultiResult multiResult = new MultiResult();
        Iterator<JsonValue> it = arr.iterator();
        String attr = paths.size() > index ? paths.get(index) : null;
        while (it.hasNext()) {
            JsonValue value = it.next();
            if (attr != null) {
                try {
                    JsonValue found = value.asObject().get(attr);
                    if (found != null) {
                        multiResult.add(convertFromJsonValue(found));
                    }
                } catch (UnsupportedOperationException ex) {
                    ignore(ex);
                }
            } else {
                multiResult.add(convertFromJsonValue(value));
            }
        }
        return multiResult;
    }

    public static Object convertFromJsonValue(JsonValue value) {
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

    private List<String> getPath(String attributePath) {
        List<String> paths = new ArrayList<String>();
        Matcher matcher = PATTERN.matcher(attributePath);
        while (matcher.find()) {
            paths.add(matcher.group());
        }
        return paths;
    }
}
