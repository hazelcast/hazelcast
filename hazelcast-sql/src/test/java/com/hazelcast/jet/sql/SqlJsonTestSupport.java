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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class SqlJsonTestSupport extends SqlTestSupport {
    protected static final ObjectMapper SERIALIZER = new ObjectMapper();

    protected void initComplexObject() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");
        test.put(1L, new HazelcastJsonValue("["
                + "[1,\"2\",3,{\"t\":1}],"
                + "{\"t\":1},"
                + "3"
                + "]"));
    }

    protected Object querySingleValue(final String sql) {
        final Map<String, Object> result = querySingleRow(sql);
        return result.values().iterator().next();
    }

    protected Map<String, Object> querySingleRow(final String sql) {
        return query(sql).get(0);
    }

    protected List<Map<String, Object>> query(final String sql) {
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

    protected void execute(final String sql, final Object ...arguments) {
        instance().getSql().execute(sql, arguments);
    }

    protected HazelcastJsonValue jsonObj(Object ...values) {
        if ((values.length % 2) != 0) {
            throw new HazelcastException("Number of value args is not divisible by 2");
        }
        final Map<String, Object> objectMap = new HashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            objectMap.put((String) values[i], values[i + 1]);
        }

        return json(serialize(objectMap));
    }

    protected String serialize(Object val) {
        try {
            return SERIALIZER.writeValueAsString(val);
        } catch (Exception exception) {
            throw new HazelcastException(exception);
        }
    }

    protected HazelcastJsonValue json(final String value) {
        return new HazelcastJsonValue(value);
    }

    protected void assertRowsWithType(final String sql, List<SqlColumnType> expectedTypes, List<Row> expectedRows) {
        final SqlResult result = instance().getSql().execute(sql);
        final SqlRowMetadata rowMetadata = result.getRowMetadata();
        final List<SqlColumnType> actualTypes = rowMetadata.getColumns().stream()
                .map(SqlColumnMetadata::getType)
                .collect(Collectors.toList());

        final List<Row> actualRows = new ArrayList<>();
        for (final SqlRow row : result) {
            final Object[] rowValues = new Object[rowMetadata.getColumnCount()];
            for (int i = 0; i < rowMetadata.getColumnCount(); i++) {
                rowValues[i] = row.getObject(i);
            }
            actualRows.add(new Row(rowValues));
        }

        assertThat(actualTypes).containsExactlyElementsOf(expectedTypes);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(expectedRows);
    }

    // maybe worth moving to base class
    protected List<Row> rows(final int rowLength, final Object ...values) {
        if ((values.length % rowLength) != 0) {
            throw new HazelcastException("Number of row value args is not divisible by row length");
        }

        final List<Row> rowList = new ArrayList<>();
        for (int i = 0; i < values.length; i += rowLength) {
            Object[] rowValues = new Object[rowLength];
            System.arraycopy(values, i, rowValues, 0, rowLength);
            rowList.add(new Row(rowValues));
        }

        return rowList;
    }
}
