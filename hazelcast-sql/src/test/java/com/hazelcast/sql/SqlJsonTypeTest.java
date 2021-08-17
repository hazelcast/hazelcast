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

package com.hazelcast.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

@Category(SlowTest.class)
public class SqlJsonTypeTest extends SqlTestSupport {
    private static final ObjectMapper SERIALIZER = new ObjectMapper();

    @BeforeClass
    public static void beforeClass() {
        final Config config = new Config();
        config.getJetConfig().setEnabled(true);
        initialize(1, config);
    }

    @Test
    public void when_insertedIntoExistingMap_typeIsCorrect() {
        final IMap<Long, HazelcastJsonValue> test = instance().getMap("test");

        test.put(1L, json("[1,2,3]"));
        assertRowsWithType("SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 1L, json("[1,2,3]")));

        execute("INSERT INTO test VALUES (2, CAST('[4,5,6]' AS JSON))");
        assertRowsWithType("SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 1L, json("[1,2,3]"), 2L, json("[4,5,6]")));

        execute("DELETE FROM test WHERE __key = 1");
        assertRowsWithType("SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 2L, json("[4,5,6]")));
    }

    @Test
    public void when_insertNewIntoMappingBasedMap_typeIsCorrect() {
        execute("CREATE MAPPING test (__key BIGINT, this JSON) "
                + "TYPE IMap "
                + "OPTIONS ('keyFormat'='bigint', 'valueFormat'='json_type')");

        execute("INSERT INTO test VALUES (1, CAST('[1,2,3]' AS JSON))");
        assertRowsWithType("SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 1L, json("[1,2,3]")));

        execute("INSERT INTO test VALUES (2, CAST('[4,5,6]' AS JSON))");
        assertRowsWithType("SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 1L, json("[1,2,3]"), 2L, json("[4,5,6]")));

        execute("DELETE FROM test WHERE __key = 1");
        assertRowsWithType("SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 2L, json("[4,5,6]")));
    }

    @Test
    public void when_sinkIsUsedWithExistingMap_typeIsCorrect() {
        execute("CREATE MAPPING test (__key BIGINT, this JSON) "
                + "TYPE IMap "
                + "OPTIONS ('keyFormat'='bigint', 'valueFormat'='json_type')");

        execute("INSERT INTO test VALUES (1, CAST('[1,2,3]' AS JSON))");
        assertRowsWithType("SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 1L, json("[1,2,3]")));

        execute("SINK INTO test SELECT 1, CAST('[4,5,6]' AS JSON)");
        assertRowsWithType("SELECT * FROM test" ,
                asList(SqlColumnType.BIGINT, SqlColumnType.JSON),
                rows(2, 1L, json("[4,5,6]")));
    }

    private void execute(final String sql, final Object ...arguments) {
        instance().getSql().execute(sql, arguments);
    }

    private HazelcastJsonValue jsonObj(Object ...values) {
        if ((values.length % 2) != 0) {
            throw new HazelcastException("Number of value args is not divisible by 2");
        }
        final Map<String, Object> objectMap = new HashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            objectMap.put((String) values[i], values[i + 1]);
        }

        return json(serialize(objectMap));
    }

    private String serialize(Object val) {
        try {
            return SERIALIZER.writeValueAsString(val);
        } catch (Exception exception) {
            throw new HazelcastException(exception);
        }
    }

    private HazelcastJsonValue json(final String value) {
        return new HazelcastJsonValue(value);
    }

    private void assertRowsWithType(final String sql, List<SqlColumnType> expectedTypes, List<Row> expectedRows) {
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
    private List<Row> rows(final int rowLength, final Object ...values) {
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
