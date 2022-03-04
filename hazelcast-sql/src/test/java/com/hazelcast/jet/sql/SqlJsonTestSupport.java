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

package com.hazelcast.jet.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public abstract class SqlJsonTestSupport extends SqlTestSupport {
    private static final ObjectMapper SERIALIZER = new ObjectMapper();

    public static HazelcastJsonValue json(final String value) {
        return new HazelcastJsonValue(value);
    }

    public static Object querySingleValue(final String sql) {
        final List<Map<String, Object>> rows = query(sql);
        assertEquals(1, rows.size());

        final Map<String, Object> row = rows.get(0);
        assertEquals(1, row.size());

        return row.values().iterator().next();
    }

    public static List<Map<String, Object>> query(final String sql) {
        final List<Map<String, Object>> results = new ArrayList<>();

        for (final SqlRow row : instance().getSql().execute(sql)) {
            final Map<String, Object> result = new HashMap<>();
            final SqlRowMetadata rowMetadata = row.getMetadata();
            for (int i = 0; i < rowMetadata.getColumnCount(); i++) {
                result.put(rowMetadata.getColumn(i).getName(), row.getObject(i));
            }

            results.add(result);
        }

        return results;
    }

    public static Map<Object, Object> objectMap(Object... kvPairs) {
        final Map<Object, Object> result = new LinkedHashMap<>();

        for (int i = 0; i < kvPairs.length; i += 2) {
            result.put(kvPairs[i], kvPairs[i + 1]);
        }

        return result;
    }

    public static String jsonString(Object value) {
        try {
            return SERIALIZER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new HazelcastException("Unable to serialize value: ", e);
        }
    }
}
