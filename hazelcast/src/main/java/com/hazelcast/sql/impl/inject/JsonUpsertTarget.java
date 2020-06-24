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

package com.hazelcast.sql.impl.inject;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class JsonUpsertTarget implements UpsertTarget {

    JsonUpsertTarget() {
    }

    @Override
    public Target get() {
        // TODO: reuse ???
        return new JsonTarget();
    }

    @Override
    public UpsertInjector createInjector(String path) {
        return (target, value) -> ((JsonTarget) target).put(path, value);
    }

    private static final class JsonTarget implements Target {

        private final Map<String, Object> entries;

        private JsonTarget() {
            this.entries = new LinkedHashMap<>();
        }

        private void put(String key, Object value) {
            entries.put(key, value);
        }

        @Override
        public Object conclude() {
            // TODO:
            StringBuilder builder = new StringBuilder("{");
            int i = 0;
            for (Entry<String, Object> entry : entries.entrySet()) {
                if (i > 0) {
                    builder.append(',');
                }

                builder.append('"').append(entry.getKey()).append('"');

                builder.append(':');

                Object value = entry.getValue();
                if (value == null) {
                    builder.append("null");
                } else if ((value instanceof Number && !(value instanceof BigDecimal) && !(value instanceof BigInteger))
                        || value instanceof Boolean) {
                    builder.append(QueryDataType.VARCHAR.convert(value));
                } else {
                    builder.append('"').append(QueryDataType.VARCHAR.convert(value)).append('"');
                }

                i++;
            }
            builder.append("}");
            return new HazelcastJsonValue(builder.toString());
        }
    }
}
