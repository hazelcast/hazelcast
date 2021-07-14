/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.extract;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.json.JsonUtil;
import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.Map;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

@NotThreadSafe
public class JsonQueryTarget implements QueryTarget {

    private Map<String, Object> json;

    @Override
    @SuppressWarnings("unchecked")
    public void setTarget(Object target, Data targetData) {
        assert targetData == null;
        try {
            json = target instanceof Map ? (Map<String, Object>) target : JsonUtil.mapFrom(target);
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
        return () -> type.convert(json.get(path));
    }
}
