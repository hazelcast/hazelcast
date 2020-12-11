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

package com.hazelcast.jet.sql.impl.extract;

import com.fasterxml.jackson.jr.stree.JrsBoolean;
import com.fasterxml.jackson.jr.stree.JrsNumber;
import com.fasterxml.jackson.jr.stree.JrsObject;
import com.fasterxml.jackson.jr.stree.JrsValue;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

@NotThreadSafe
public class JsonQueryTarget implements QueryTarget {

    private JrsObject json;

    @Override
    public void setTarget(Object target) {
        try {
            json = target instanceof JrsObject ? (JrsObject) target : JsonUtil.treeFrom(target);
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public QueryExtractor createExtractor(String path, QueryDataType type) {
        return path == null ? createExtractor() : createFieldExtractor(path, type);
    }

    private QueryExtractor createExtractor() {
        return () -> json;
    }

    private QueryExtractor createFieldExtractor(String path, QueryDataType type) {
        return () -> type.convert(extractValue(json, path));
    }

    private static Object extractValue(JrsObject json, String path) {
        JrsValue value = json.get(path);
        if (value == null || value.isNull()) {
            return null;
        } else if (value instanceof JrsBoolean) {
            return ((JrsBoolean) value).booleanValue();
        } else if (value.isNumber()) {
            return ((JrsNumber) value).getValue();
        } else if (value.isValueNode()) {
            return value.asText();
        } else {
            return value;
        }
    }
}
